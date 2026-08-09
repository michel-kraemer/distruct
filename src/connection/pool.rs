use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use dashmap::DashMap;
use quinn::{
    ClientConfig, Endpoint, ServerConfig,
    crypto::rustls::{QuicClientConfig, QuicServerConfig},
    rustls::{
        self, RootCertStore,
        pki_types::{CertificateDer, PrivateKeyDer},
    },
};

use crate::{
    ALPN_QUIC_CLUSTER,
    connection::{
        client::{Client, ClientConnectError},
        server::Server,
    },
};

pub struct Pool {
    endpoint: Endpoint,
    local_addr: SocketAddr,
    connections: Arc<DashMap<SocketAddr, Client>>,
}

impl Pool {
    pub fn new(
        addr: SocketAddr,
        certs: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Result<Self> {
        // create client configuration
        let mut store = RootCertStore::empty();
        for cert in &certs {
            store.add(cert.clone())?;
        }

        let mut client_crypto = rustls::ClientConfig::builder()
            .with_root_certificates(store)
            .with_no_client_auth();

        client_crypto.alpn_protocols = vec![ALPN_QUIC_CLUSTER.to_vec()];

        let client_config = ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_crypto)?));

        // create server configuration
        let mut server_crypto = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)?;

        server_crypto.alpn_protocols = vec![ALPN_QUIC_CLUSTER.to_vec()];

        let mut server_config =
            ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server_crypto)?));
        let transport_config = Arc::get_mut(&mut server_config.transport).unwrap();
        transport_config.max_concurrent_uni_streams(0_u8.into());

        // create endpoint
        let mut endpoint = Endpoint::server(server_config, addr)?;
        endpoint.set_default_client_config(client_config);
        let local_addr = endpoint.local_addr()?;

        Ok(Self {
            endpoint,
            local_addr,
            connections: Arc::new(DashMap::new()),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn spawn_server(&self) -> Server {
        Server::new(self.endpoint.clone())
    }

    pub async fn connect(
        &self,
        host: SocketAddr,
        server_name: &str,
    ) -> Result<Client, ClientConnectError> {
        if let Some(cached) = self.connections.get(&host)
            && cached.is_open()
        {
            // return cached connection
            return Ok(cached.clone());
        }

        // create new connection
        let result = Client::new(host, server_name, &self.endpoint).await?;
        self.connections.insert(host, result.clone());

        {
            let result = result.clone();
            let connections = Arc::clone(&self.connections);
            tokio::spawn(async move {
                // wait for the connection to close
                result.closed().await;

                // remove it from `connections` but only if the value in the map
                // is really closed, i.e. if no new connection was added in the
                // meantime
                connections.remove_if(&host, |_, old| !old.is_open());
            });
        }

        Ok(result)
    }

    pub fn force_remove(&self, host: SocketAddr) {
        self.connections.remove(&host);
    }
}
