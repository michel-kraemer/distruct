use std::time::Instant;

use dashmap::DashMap;

use crate::raft::NodeId;

#[derive(Default)]
pub(crate) struct HeartbeatMonitor {
    last_seen: DashMap<NodeId, Instant>,
}

impl HeartbeatMonitor {
    pub(super) fn on_heartbeat(&self, id: NodeId) {
        self.last_seen.insert(id, Instant::now());
    }

    pub(crate) fn last_seen(&self, id: NodeId) -> Instant {
        *self.last_seen.entry(id).or_insert_with(Instant::now)
    }

    pub(crate) fn retain<F>(&self, predicate: F)
    where
        F: Fn(NodeId) -> bool,
    {
        self.last_seen.retain(|k, _| predicate(*k));
    }
}
