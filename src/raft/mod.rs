use std::io::Cursor;

use openraft::{RaftTypeConfig, TokioRuntime, impls::OneshotResponder};
use serde::{Deserialize, Serialize};

use crate::raft::node::Node;

pub mod heartbeat;
pub mod log;
pub mod network;
pub mod node;
pub mod state;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientRequest {
    Set { key: String, value: String },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientResponse {
    value: Option<String>,
}

#[cfg(not(test))]
pub type NodeId = ulid::Ulid;
#[cfg(test)]
pub type NodeId = u64;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct TypeConfig {}

impl RaftTypeConfig for TypeConfig {
    type D = ClientRequest;
    type R = ClientResponse;
    type NodeId = NodeId;
    type Node = Node;
    type Entry = openraft::Entry<Self>;
    type SnapshotData = Cursor<Vec<u8>>;
    type AsyncRuntime = TokioRuntime;
    type Responder = OneshotResponder<Self>;
}

#[cfg(test)]
mod tests {
    use openraft::{StorageError, testing::StoreBuilder};

    use crate::raft::{NodeId, TypeConfig, log::LogStorage, state::StateMachine};

    struct MyStoreBuilder {}

    impl StoreBuilder<TypeConfig, LogStorage<TypeConfig>, StateMachine> for MyStoreBuilder {
        async fn build(
            &self,
        ) -> Result<((), LogStorage<TypeConfig>, StateMachine), StorageError<NodeId>> {
            Ok(((), LogStorage::default(), StateMachine::default()))
        }
    }

    #[test]
    fn test_mem_store() -> anyhow::Result<()> {
        Ok(openraft::testing::Suite::test_all(MyStoreBuilder {})?)
    }
}
