use std::{borrow::Borrow, marker::PhantomData, sync::Arc};

use serde::{Deserialize, Serialize};

use super::{ReadConsistency, should_read_stale};
use crate::{
    cluster::ClusterRef,
    connection::client::Client,
    error::{Error, Result},
};

/// A distributed map that can store multiple values per key.
///
/// Keys and values must implement [`Serialize`] and [`Deserialize`].
///
/// A multi-map stores multiple values per key, much like a
/// [`HashMap`](std::collections::HashMap) whose values are
/// [`HashSet<V>`](std::collections::HashSet)s. Duplicate key-value pairs are
/// prevented. The order of inserted values is not preserved.
///
/// Create a distributed multi-map through
/// [`Cluster::get_multi_map`](crate::Cluster::get_multi_map) as follows:
///
/// ```no_run
/// # use distruct::collections::DMultiMap;
/// # fn get_multi_map(cluster: distruct::Cluster) {
/// let multi_map: DMultiMap<String, String> = cluster.get_multi_map("my_multi_map");
/// # }
pub struct DMultiMap<K, V> {
    name: String,
    cluster: Arc<ClusterRef>,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> DMultiMap<K, V>
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

    /// Insert a key-value pair into the map.
    ///
    /// Existing values will not be replaced.
    ///
    /// The method returns `true` if the value was inserted into the map or
    /// `false` if it already existed.
    pub async fn insert(&self, key: K, value: V) -> Result<bool> {
        self.client_to_leader()
            .await?
            .insert_multi(
                &self.name,
                postcard::to_allocvec(&key)?,
                postcard::to_allocvec(&value)?,
            )
            .await
    }

    /// Get all values for a given `key` from the map but accept a stale result.
    ///
    /// If the map does not contain the `key`, return an empty [`Vec`].
    ///
    /// This is similar to calling [`get_with`](Self::get_with) using
    /// [`ReadConsistency::Stale`].
    ///
    /// The method accesses the current node's local copy of the map. There is
    /// no communication with other nodes in the cluster. The node may be
    /// arbitrarily behind the leader and the result might be stale.
    pub async fn get_stale<Q>(&self, key: &Q) -> Result<Vec<V>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serialize,
    {
        let key = postcard::to_allocvec(key)?;
        Ok(self
            .cluster
            .state_machine()
            .get_with_lock_multi(&self.name, &key)
            .await
            .map(|set| {
                set.iter()
                    .map(|v| Ok::<_, Error>(postcard::from_bytes(v)?))
                    .collect()
            })
            .transpose()?
            .unwrap_or_default())
    }

    /// Get all values for a given `key` from the map using a given [read
    /// consistency guarantee](ReadConsistency).
    ///
    /// If the map does not contain the `key`, return an empty [`Vec`].
    pub async fn get_with<Q>(&self, key: &Q, consistency: ReadConsistency) -> Result<Vec<V>>
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
                .get_multi(&self.name, postcard::to_allocvec(key)?, consistency)
                .await?
                .map(|set| {
                    set.iter()
                        .map(|v| Ok::<_, Error>(postcard::from_bytes(v)?))
                        .collect()
                })
                .transpose()?
                .unwrap_or_default())
        }
    }

    /// Get all values for a given `key` from the map using strong consistency.
    ///
    /// If the map does not contain the `key`, return an empty [`Vec`].
    ///
    /// This is similar to calling [`get_with`](Self::get_with) using
    /// [`ReadConsistency::ReadIndex`].
    pub async fn get<Q>(&self, key: &Q) -> Result<Vec<V>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serialize,
    {
        self.get_with(key, ReadConsistency::ReadIndex).await
    }

    /// Remove a key-value pair from the map.
    ///
    /// Other values with the same key are retained.
    ///
    /// Returns `true` if the key-value pair was actually present in the map.
    pub async fn remove<Q, U>(&self, key: &Q, value: &U) -> Result<bool>
    where
        K: Borrow<Q>,
        V: Borrow<U>,
        Q: ?Sized + Serialize,
        U: ?Sized + Serialize,
    {
        self.client_to_leader()
            .await?
            .remove_multi(
                &self.name,
                postcard::to_allocvec(key)?,
                postcard::to_allocvec(value)?,
            )
            .await
    }
}
