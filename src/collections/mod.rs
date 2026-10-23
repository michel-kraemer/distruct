//! Distributed collections.
//!
//! Create instances of distributed collections through one of the methods
//! provided by [`Cluster`](crate::Cluster). For instance:
//!
//! ```no_run
//! # use distruct::collections::DMap;
//! # fn get_map(cluster: distruct::Cluster) {
//! let map: DMap<String, String> = cluster.get_map("my_distributed_map");
//! # }
//! ```
mod dmap;
mod dmultimap;

use std::sync::Arc;

pub use dmap::DMap;
pub use dmultimap::DMultiMap;
use openraft::{
    Raft,
    error::{CheckIsLeaderError, RaftError},
};
use serde::{Deserialize, Serialize};

use crate::{
    cluster::ClusterRef,
    error::Result,
    raft::{
        TypeConfig,
        node::{Node, NodeId},
    },
};

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
/// Specifies consistency guarantees when reading from a distributed collection
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

async fn ensure_consistency(
    consistency: ReadConsistency,
    raft: &Arc<Raft<TypeConfig>>,
) -> Result<(), RaftError<NodeId, CheckIsLeaderError<NodeId, Node>>> {
    match consistency {
        ReadConsistency::Stale | ReadConsistency::LeaderStale => Ok(()),
        ReadConsistency::LeaseRead | ReadConsistency::ReadIndex => {
            raft.ensure_linearizable().await.map(|_| ())
        }
    }
}

// Check whether a read for `consistency` can be served from local state instead
// of being forwarded to the leader via a client
async fn should_read_stale(consistency: ReadConsistency, cluster: &ClusterRef) -> Result<bool> {
    match consistency {
        ReadConsistency::Stale => {
            // stale read is allowed
            Ok(true)
        }

        ReadConsistency::LeaderStale | ReadConsistency::LeaseRead | ReadConsistency::ReadIndex => {
            if !cluster.is_leader().await {
                // we must not read from local state
                return Ok(false);
            }

            match ensure_consistency(consistency, cluster.raft()).await {
                Ok(()) => {
                    // We are the leader and we were able to ensure consistency.
                    // We can read from local state.
                    Ok(true)
                }

                Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(_))) => {
                    // We thought we were the leader, but we aren't. We must not
                    // read a possibly stale state.
                    Ok(false)
                }

                Err(err) => Err(err.into()),
            }
        }
    }
}
