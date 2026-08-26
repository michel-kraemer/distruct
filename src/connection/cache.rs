use std::{net::SocketAddr, sync::Arc};

use dashmap::DashMap;
use quinn::{Connection, Endpoint};

use crate::{Result, error::TransportError, raft::node::Node};

#[derive(Clone)]
pub(crate) struct ConnectionCache {
    endpoint: Endpoint,
    connections: Arc<DashMap<SocketAddr, Connection>>,
}

impl ConnectionCache {
    pub(crate) fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            connections: Arc::new(DashMap::new()),
        }
    }

    pub(crate) async fn connect(&self, node: &Node) -> Result<Connection> {
        if let Some(cached) = self.connections.get(&node.addr())
            && cached.close_reason().is_none()
        {
            // return cached connection
            return Ok(cached.clone());
        }

        // create new connection
        let host = node.addr();
        let result = self
            .endpoint
            .connect(node.addr(), node.server_name())
            .map_err(TransportError::from)?
            .await
            .map_err(TransportError::from)?;
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
                connections.remove_if(&host, |_, old| old.close_reason().is_some());
            });
        }

        Ok(result)
    }

    pub(crate) fn force_remove(&self, host: SocketAddr) {
        self.connections.remove(&host);
    }
}
