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
pub enum RaftRequest {
    Insert {
        map: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Clear {
        map: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RaftResponse {
    pub value: Option<Vec<u8>>,
}

#[cfg(not(test))]
pub type NodeId = ulid::Ulid;
#[cfg(test)]
pub type NodeId = u64;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct TypeConfig {}

impl RaftTypeConfig for TypeConfig {
    type D = RaftRequest;
    type R = RaftResponse;
    type NodeId = NodeId;
    type Node = Node;
    type Entry = openraft::Entry<Self>;
    type SnapshotData = Cursor<Vec<u8>>;
    type AsyncRuntime = TokioRuntime;
    type Responder = OneshotResponder<Self>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use openraft::{StorageError, testing::StoreBuilder};

    use crate::raft::{NodeId, TypeConfig, log::LogStorage, state::StateMachine};

    struct MyStoreBuilder {}

    impl StoreBuilder<TypeConfig, LogStorage<TypeConfig>, Arc<StateMachine>> for MyStoreBuilder {
        async fn build(
            &self,
        ) -> Result<((), LogStorage<TypeConfig>, Arc<StateMachine>), StorageError<NodeId>> {
            Ok(((), LogStorage::default(), Arc::new(StateMachine::default())))
        }
    }

    #[test]
    fn test_mem_store() -> anyhow::Result<()> {
        Ok(openraft::testing::Suite::test_all(MyStoreBuilder {})?)
    }
}
