use std::{borrow::Borrow, marker::PhantomData, sync::Arc};

use openraft::{
    Raft,
    error::{CheckIsLeaderError, RaftError},
};
use serde::{Deserialize, Serialize};

use crate::{
    Error, Result,
    cluster::Cluster,
    collections::ReadConsistency,
    connection::client::Client,
    raft::{
        TypeConfig,
        node::{Node, NodeId},
    },
};

pub struct DMap<'c, K, V> {
    name: String,
    cluster: &'c Cluster,
    _marker: PhantomData<(K, V)>,
}

impl<'c, K, V> DMap<'c, K, V>
where
    K: Serialize,
    V: Serialize + for<'a> Deserialize<'a>,
{
    pub(crate) fn new<N>(name: N, cluster: &'c Cluster) -> Self
    where
        N: Into<String>,
    {
        Self {
            name: name.into(),
            cluster,
            _marker: PhantomData,
        }
    }

    async fn client_to_leader(&self) -> Result<Client> {
        let (leader_id, leader) = self.cluster.get_leader().ok_or(Error::LeaderNotFound)?;
        Client::new(
            &leader,
            Some(leader_id),
            Arc::clone(self.cluster.connection_cache()),
        )
        .await
    }

    pub async fn contains_key_stale<Q>(&self, k: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serialize,
    {
        let key = postcard::to_allocvec(k)?;
        Ok(self
            .cluster
            .state_machine()
            .contains_key(&self.name, &key)
            .await)
    }

    pub async fn contains_key_with<Q>(&self, k: &Q, consistency: ReadConsistency) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serialize,
    {
        if should_read_stale(consistency, self.cluster).await? {
            self.contains_key_stale(k).await
        } else {
            self.client_to_leader()
                .await?
                .contains_key(&self.name, postcard::to_allocvec(k)?, consistency)
                .await
        }
    }

    pub async fn contains_key<Q>(&self, k: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serialize,
    {
        self.contains_key_with(k, ReadConsistency::ReadIndex).await
    }

    pub async fn insert(&self, k: K, v: V) -> Result<Option<V>> {
        Ok(self
            .client_to_leader()
            .await?
            .insert(
                &self.name,
                postcard::to_allocvec(&k)?,
                postcard::to_allocvec(&v)?,
            )
            .await?
            .map(|value| postcard::from_bytes(&value))
            .transpose()?)
    }

    pub async fn get_stale<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serialize,
    {
        let key = postcard::to_allocvec(k)?;
        self.cluster
            .state_machine()
            .get_with_lock(&self.name, &key)
            .await
            .map(|value| Ok(postcard::from_bytes(&value)?))
            .transpose()
    }

    pub async fn get_with<Q>(&self, k: &Q, consistency: ReadConsistency) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serialize,
    {
        if should_read_stale(consistency, self.cluster).await? {
            self.get_stale(k).await
        } else {
            Ok(self
                .client_to_leader()
                .await?
                .get(&self.name, postcard::to_allocvec(k)?, consistency)
                .await?
                .map(|value| postcard::from_bytes(&value))
                .transpose()?)
        }
    }

    pub async fn get<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serialize,
    {
        self.get_with(k, ReadConsistency::ReadIndex).await
    }

    pub async fn remove<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serialize,
    {
        Ok(self
            .client_to_leader()
            .await?
            .remove(&self.name, postcard::to_allocvec(k)?)
            .await?
            .map(|value| postcard::from_bytes(&value))
            .transpose()?)
    }

    pub async fn len_stale(&self) -> usize {
        self.cluster
            .state_machine()
            .map_len(&self.name)
            .await
            .unwrap_or_default()
    }

    pub async fn len_with(&self, consistency: ReadConsistency) -> Result<usize> {
        if should_read_stale(consistency, self.cluster).await? {
            Ok(self.len_stale().await)
        } else {
            let result = self
                .client_to_leader()
                .await?
                .len(&self.name, consistency)
                .await?;
            Ok(result.unwrap_or_default())
        }
    }

    pub async fn len(&self) -> Result<usize> {
        self.len_with(ReadConsistency::ReadIndex).await
    }

    pub async fn is_empty_stale(&self) -> bool {
        self.len_stale().await == 0
    }

    pub async fn is_empty_with(&self, consistency: ReadConsistency) -> Result<bool> {
        Ok(self.len_with(consistency).await? == 0)
    }

    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    pub async fn clear(&self) -> Result<()> {
        self.client_to_leader().await?.clear(&self.name).await
    }
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
async fn should_read_stale(consistency: ReadConsistency, cluster: &Cluster) -> Result<bool> {
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
