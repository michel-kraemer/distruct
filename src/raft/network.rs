use std::sync::Arc;

use openraft::{
    RaftNetwork, RaftNetworkFactory,
    error::{InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable},
    network::RPCOption,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};

use crate::{
    connection::{
        client::{Client, ClientRaftError, ClientRequestError},
        pool::Pool,
    },
    raft::{NodeId, TypeConfig, heartbeat::HeartbeatMonitor, node::Node},
};

pub struct Network {
    node_id: NodeId,
    node: Node,
    heartbeat_monitor: Arc<HeartbeatMonitor>,
    pool: Arc<Pool>,
}

impl Network {
    fn new(
        node_id: NodeId,
        node: Node,
        heartbeat_monitor: Arc<HeartbeatMonitor>,
        pool: Arc<Pool>,
    ) -> Self {
        Self {
            node_id,
            node,
            heartbeat_monitor,
            pool,
        }
    }

    async fn perform_raft_action<R, O>(
        &mut self,
        operation: O,
        option: RPCOption,
    ) -> Result<R, RPCError<NodeId, Node, RaftError<NodeId>>>
    where
        O: AsyncFnOnce(Client) -> Result<R, ClientRaftError>,
    {
        let client = self
            .pool
            .connect(self.node.addr(), self.node.server_name())
            .await
            .map_err(|e| Unreachable::new(&e))?;

        let timeout = tokio::time::timeout(option.soft_ttl(), operation(client)).await;
        let result = match timeout {
            Ok(result) => result,
            Err(e) => {
                // Connection to client has timed out. Remove it from the pool
                // so we don't try to use it again.
                self.pool.force_remove(self.node.addr());
                return Err(RPCError::from(Unreachable::new(&e)));
            }
        };

        match result {
            Ok(result) => {
                // let the heartbeat monitor know that we just had contact with the node
                // and that the request was successful
                self.heartbeat_monitor.on_heartbeat(self.node_id);

                Ok(result)
            }

            Err(e) => Err(match e {
                ClientRaftError::Raft(re) => RPCError::from(RemoteError::new_with_node(
                    self.node_id,
                    self.node.clone(),
                    re,
                )),
                ClientRaftError::Request(re) => match re {
                    ClientRequestError::Connection(_)
                    | ClientRequestError::DeserializeResponse(_)
                    | ClientRequestError::SerializeRequest(_) => {
                        RPCError::from(Unreachable::new(&re))
                    }
                    ClientRequestError::ReadToEnd(_) | ClientRequestError::Write(_) => {
                        RPCError::from(NetworkError::new(&re))
                    }
                },
                ClientRaftError::UnknownResponse(_) => RPCError::from(Unreachable::new(&e)),
            }),
        }
    }
}

impl RaftNetwork<TypeConfig> for Network {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        self.perform_raft_action(async |client| client.append_entries(rpc).await, option)
            .await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, Node, RaftError<NodeId, InstallSnapshotError>>,
    > {
        todo!()
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        self.perform_raft_action(async |client| client.vote(rpc).await, option)
            .await
    }
}

pub struct NetworkFactory {
    heartbeat_monitor: Arc<HeartbeatMonitor>,
    pool: Arc<Pool>,
}

impl NetworkFactory {
    pub fn new(heartbeat_monitor: Arc<HeartbeatMonitor>, pool: Arc<Pool>) -> Self {
        Self {
            heartbeat_monitor,
            pool,
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = Network;

    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        Network::new(
            target,
            node.clone(),
            Arc::clone(&self.heartbeat_monitor),
            Arc::clone(&self.pool),
        )
    }
}
