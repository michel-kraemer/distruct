use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use dashmap::DashMap;
use quinn::Endpoint;

use crate::{
    connection::client::{Client, ClientConnectError},
    raft::node::{Node, NodeId},
};

pub(crate) struct ConnectionCache {
    endpoint: Endpoint,
    connections: Arc<DashMap<SocketAddr, Client>>,
}

impl ConnectionCache {
    pub(crate) fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            connections: Arc::new(DashMap::new()),
        }
    }

    pub(crate) async fn connect(
        &self,
        node: &Node,
        node_id: Option<NodeId>,
    ) -> Result<Client, ClientConnectError> {
        if let Some(cached) = self.connections.get(&node.addr())
            && cached.is_open()
        {
            // return cached connection
            return Ok(cached.clone());
        }

        // create new connection
        let host = node.addr();
        let result = Client::new(node, node_id, &self.endpoint).await?;
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

    pub(crate) fn force_remove(&self, host: SocketAddr) {
        self.connections.remove(&host);
    }
}
