use std::{
    collections::HashMap,
    io::Cursor,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use openraft::{
    EntryPayload, LogId, RaftSnapshotBuilder, RaftTypeConfig, Snapshot, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership, storage::RaftStateMachine,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock, RwLockReadGuard};

use crate::raft::{RaftRequest, RaftResponse, TypeConfig};

type NodeId = <TypeConfig as RaftTypeConfig>::NodeId;
type Node = <TypeConfig as RaftTypeConfig>::Node;
type Entry = <TypeConfig as RaftTypeConfig>::Entry;
type SnapshotData = <TypeConfig as RaftTypeConfig>::SnapshotData;

struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, Node>,

    /// The data of the state machine at the time of this snapshot.
    data: Vec<u8>,
}

#[derive(Default, Serialize, Deserialize)]
struct StateMachineData {
    maps: HashMap<String, HashMap<Vec<u8>, Vec<u8>>>,
}

#[derive(Default)]
struct StateMachineInner {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, Node>,
    data: StateMachineData,
}

#[derive(Default)]
pub(crate) struct StateMachine {
    /// The Raft state machine.
    state_machine: RwLock<StateMachineInner>,

    /// Snapshot identifier
    snapshot_idx: AtomicU64,

    /// The last received snapshot
    current_snapshot: RwLock<Option<StoredSnapshot>>,
}

impl StateMachine {
    pub(crate) async fn contains_key(&self, map: &str, key: &[u8]) -> bool {
        let sm = self.state_machine.read().await;
        sm.data
            .maps
            .get(map)
            .map(|m| m.contains_key(key))
            .unwrap_or_default()
    }

    pub(crate) async fn get_with_lock(
        &self,
        map: &str,
        key: &[u8],
    ) -> Option<RwLockReadGuard<'_, Vec<u8>>> {
        let sm = self.state_machine.read().await;
        RwLockReadGuard::try_map(sm, |sm| sm.data.maps.get(map).and_then(|m| m.get(key))).ok()
    }

    pub(crate) async fn remove(&self, map: &str, key: &[u8]) -> Option<Vec<u8>> {
        let mut sm = self.state_machine.write().await;
        sm.data.maps.get_mut(map).and_then(|m| m.remove(key))
    }

    pub(crate) async fn map_len(&self, map: &str) -> Option<usize> {
        self.state_machine
            .read()
            .await
            .data
            .maps
            .get(map)
            .map(|m| m.len())
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachine> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, Node>), StorageError<NodeId>> {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership.clone(),
        ))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<RaftResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry>,
    {
        let mut sm = self.state_machine.write().await;

        let mut res = Vec::new();
        for entry in entries {
            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(RaftResponse { value: None }),
                EntryPayload::Normal(req) => match req {
                    RaftRequest::Insert { map, key, value } => {
                        let m = sm.data.maps.entry(map).or_default();
                        let old = m.insert(key, value.clone());
                        res.push(RaftResponse { value: old });
                    }
                    RaftRequest::Clear { map } => {
                        let m = sm.data.maps.entry(map).or_default();
                        m.clear();
                        res.push(RaftResponse { value: None });
                    }
                },
                EntryPayload::Membership(mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem);
                    res.push(RaftResponse { value: None });
                }
            };
        }

        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<SnapshotData>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, Node>,
        snapshot: Box<SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        // update the state machine
        let data = postcard::from_bytes(&new_snapshot.data)
            .map_err(|e| StorageIOError::read_snapshot(Some(new_snapshot.meta.signature()), &e))?;
        let updated_state_machine = StateMachineInner {
            last_applied_log: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
            data,
        };
        let mut state_machine = self.state_machine.write().await;
        *state_machine = updated_state_machine;

        // lock the current snapshot before releasing the lock on the state
        // machine, to avoid a race condition on the written snapshot
        let mut current_snapshot = self.current_snapshot.write().await;
        drop(state_machine);

        // update current snapshot
        *current_snapshot = Some(new_snapshot);

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        Ok(self
            .current_snapshot
            .read()
            .await
            .as_ref()
            .map(|snapshot| Snapshot {
                meta: snapshot.meta.clone(),
                snapshot: Box::new(Cursor::new(snapshot.data.clone())),
            }))
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachine> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        // serialize the data of the state machine
        let state_machine = self.state_machine.read().await;
        let data = postcard::to_allocvec(&state_machine.data)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;

        let last_applied_log = state_machine.last_applied_log;
        let last_membership = state_machine.last_membership.clone();

        // lock the current snapshot before releasing the lock on the state
        // machine, to avoid a race condition on the written snapshot
        let mut current_snapshot = self.current_snapshot.write().await;
        drop(state_machine);

        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        *current_snapshot = Some(snapshot);

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}
