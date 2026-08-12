use std::io::Cursor;

use openraft::{RaftTypeConfig, TokioRuntime, impls::OneshotResponder};
use serde::{Deserialize, Serialize};

use crate::raft::node::{Node, NodeId};

pub(crate) mod heartbeat;
pub(crate) mod log;
pub(crate) mod network;
pub(crate) mod node;
pub(crate) mod state;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum RaftRequest {
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
pub(crate) struct RaftResponse {
    pub(crate) value: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TypeConfig {}

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
    fn test_mem_store() -> Result<(), Box<StorageError<NodeId>>> {
        Ok(openraft::testing::Suite::test_all(MyStoreBuilder {})?)
    }
}
