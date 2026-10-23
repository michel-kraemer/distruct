use std::{borrow::Borrow, marker::PhantomData, sync::Arc};

use serde::{Deserialize, Serialize};

use super::{ReadConsistency, should_read_stale};
use crate::{
    cluster::ClusterRef,
    connection::client::Client,
    error::{Error, Result},
};

/// A distributed map.
///
/// Works similarly to a [`HashMap`](std::collections::HashMap) but its contents
/// can be accessed by multiple nodes in a [`Cluster`](crate::Cluster).
///
/// Keys and values must implement [`Serialize`] and [`Deserialize`].
///
/// Create a distributed map through
/// [`Cluster::get_map`](crate::Cluster::get_map) as follows:
///
/// ```no_run
/// # use distruct::collections::DMap;
/// # fn get_map(cluster: distruct::Cluster) {
/// let map: DMap<String, String> = cluster.get_map("my_distributed_map");
/// # }
pub struct DMap<K, V> {
    name: String,
    cluster: Arc<ClusterRef>,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> DMap<K, V>
where
    K: Serialize,
    V: Serialize + for<'a> Deserialize<'a>,
{
    pub(crate) fn new<N>(name: N, cluster: Arc<ClusterRef>) -> Self
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

    /// Determine if the map contains the given `key` but accept a stale result.
    ///
    /// This is similar to calling
    /// [`contains_key_with`](Self::contains_key_with) using
    /// [`ReadConsistency::Stale`].
    ///
    /// The method accesses the current node's local copy of the map. There is
    /// no communication with other nodes in the cluster. The node may be
    /// arbitrarily behind the leader and the result might be stale.
    pub async fn contains_key_stale<Q>(&self, key: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serialize,
    {
        let key = postcard::to_allocvec(key)?;
        Ok(self
            .cluster
            .state_machine()
            .contains_key(&self.name, &key)
            .await)
    }

    /// Determine if the map contains the given `key` using a given [read
    /// consistency guarantee](ReadConsistency).
    pub async fn contains_key_with<Q>(&self, key: &Q, consistency: ReadConsistency) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serialize,
    {
        if should_read_stale(consistency, &self.cluster).await? {
            self.contains_key_stale(key).await
        } else {
            self.client_to_leader()
                .await?
                .contains_key(&self.name, postcard::to_allocvec(key)?, consistency)
                .await
        }
    }

    /// Determine if the map contains the given `key` using strong consistency.
    ///
    /// This is similar to calling
    /// [`contains_key_with`](Self::contains_key_with) using
    /// [`ReadConsistency::ReadIndex`].
    pub async fn contains_key<Q>(&self, key: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serialize,
    {
        self.contains_key_with(key, ReadConsistency::ReadIndex)
            .await
    }

    /// Insert a key-value pair into the map.
    ///
    /// If the key already existed in the map, this method returns the old
    /// value. Otherwise, it returns [`None`].
    pub async fn insert(&self, key: K, value: V) -> Result<Option<V>> {
        Ok(self
            .client_to_leader()
            .await?
            .insert(
                &self.name,
                postcard::to_allocvec(&key)?,
                postcard::to_allocvec(&value)?,
            )
            .await?
            .map(|value| postcard::from_bytes(&value))
            .transpose()?)
    }

    /// Get the value for a given `key` from the map but accept a stale result.
    ///
    /// If the map does not contain the `key`, return [`None`].
    ///
    /// This is similar to calling [`get_with`](Self::get_with) using
    /// [`ReadConsistency::Stale`].
    ///
    /// The method accesses the current node's local copy of the map. There is
    /// no communication with other nodes in the cluster. The node may be
    /// arbitrarily behind the leader and the result might be stale.
    pub async fn get_stale<Q>(&self, key: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serialize,
    {
        let key = postcard::to_allocvec(key)?;
        self.cluster
            .state_machine()
            .get_with_lock(&self.name, &key)
            .await
            .map(|value| Ok(postcard::from_bytes(&value)?))
            .transpose()
    }

    /// Get the value for a given `key` from the map using a given [read
    /// consistency guarantee](ReadConsistency).
    ///
    /// If the map does not contain the `key`, return [`None`].
    pub async fn get_with<Q>(&self, key: &Q, consistency: ReadConsistency) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serialize,
    {
        if should_read_stale(consistency, &self.cluster).await? {
            self.get_stale(key).await
        } else {
            Ok(self
                .client_to_leader()
                .await?
                .get(&self.name, postcard::to_allocvec(key)?, consistency)
                .await?
                .map(|value| postcard::from_bytes(&value))
                .transpose()?)
        }
    }

    /// Get the value for a given `key` from the map using strong consistency.
    ///
    /// If the map does not contain the `key`, return [`None`].
    ///
    /// This is similar to calling [`get_with`](Self::get_with) using
    /// [`ReadConsistency::ReadIndex`].
    pub async fn get<Q>(&self, key: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serialize,
    {
        self.get_with(key, ReadConsistency::ReadIndex).await
    }

    /// Remove a `key` from the map.
    ///
    /// Return the old value or [`None`] if the map did not contain the given
    /// `key`.
    pub async fn remove<Q>(&self, key: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serialize,
    {
        Ok(self
            .client_to_leader()
            .await?
            .remove(&self.name, postcard::to_allocvec(key)?)
            .await?
            .map(|value| postcard::from_bytes(&value))
            .transpose()?)
    }

    /// Get the number of entries in the map but accept a stale result.
    ///
    /// This is similar to calling [`len_with`](Self::len_with) using
    /// [`ReadConsistency::Stale`].
    ///
    /// The method accesses the current node's local copy of the map. There is
    /// no communication with other nodes in the cluster. The node may be
    /// arbitrarily behind the leader and the result might be stale.
    pub async fn len_stale(&self) -> usize {
        self.cluster
            .state_machine()
            .map_len(&self.name)
            .await
            .unwrap_or_default()
    }

    /// Get the number of entries in the map using a given [read consistency
    /// guarantee](ReadConsistency).
    pub async fn len_with(&self, consistency: ReadConsistency) -> Result<usize> {
        if should_read_stale(consistency, &self.cluster).await? {
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

    /// Get the number of entries in the map using strong consistency.
    ///
    /// This is similar to calling [`len_with`](Self::len_with) using
    /// [`ReadConsistency::ReadIndex`].
    pub async fn len(&self) -> Result<usize> {
        self.len_with(ReadConsistency::ReadIndex).await
    }

    /// Determine if the map is empty but accept a stale result.
    ///
    /// This is similar to calling [`is_empty_with`](Self::is_empty_with) using
    /// [`ReadConsistency::Stale`].
    ///
    /// The method accesses the current node's local copy of the map. There is
    /// no communication with other nodes in the cluster. The node may be
    /// arbitrarily behind the leader and the result might be stale.
    pub async fn is_empty_stale(&self) -> bool {
        self.len_stale().await == 0
    }

    /// Determine if the map is empty using a given [read consistency
    /// guarantee](ReadConsistency).
    pub async fn is_empty_with(&self, consistency: ReadConsistency) -> Result<bool> {
        Ok(self.len_with(consistency).await? == 0)
    }

    /// Determine if the map is empty using strong consistency.
    ///
    /// This is similar to calling [`is_empty_with`](Self::is_empty_with) using
    /// [`ReadConsistency::ReadIndex`].
    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// Clear the map and remove all key-value pairs.
    pub async fn clear(&self) -> Result<()> {
        self.client_to_leader().await?.clear(&self.name).await
    }
}
