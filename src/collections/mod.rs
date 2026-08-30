mod dmap;

pub use dmap::DMap;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadConsistency {
    /// Read from any node's local state machine. There is no coordination and
    /// no staleness bound. The node may be arbitrarily behind the leader.
    Stale,

    /// Read from the local state machine of the node believed to be leader
    /// without confirming current leadership via quorum or lease. Usually as
    /// fresh as a linearizable read, but unlike `LeaseRead`, there is no bound
    /// on staleness if this node is a stale/partitioned leader that hasn't
    /// stepped down yet.
    LeaderStale,

    /// Linearizable under the assumption of bounded clock drift.
    ///
    /// Note: This maps to `ReadIndex` until we can upgrade the underlying
    /// OpenRaft version to 0.10.x
    LeaseRead,

    /// Linearizable via quorum confirmation on every read.
    ReadIndex,
}
