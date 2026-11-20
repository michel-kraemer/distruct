use std::{borrow::Borrow, sync::Arc};

use serde::Serialize;

use super::{DMap, ReadConsistency};
use crate::{cluster::ClusterRef, error::Result};

/// A distributed set of unique entries.
///
/// Works similarly to a [`HashSet`](std::collections::HashSet) but its
/// contents can be accessed by multiple nodes in a [`Cluster`](crate::Cluster).
/// Entries must implement [`Serialize`].
///
/// Sets are backed by distributed maps and share their namespace. Use a
/// distinct name for each set unless sharing its backing map is intentional.
///
/// Create a distributed set through [`Cluster::get_set`](crate::Cluster::get_set):
///
/// ```no_run
/// # use distruct::collections::DSet;
/// # fn get_set(cluster: distruct::Cluster) {
/// let set: DSet<String> = cluster.get_set("my_distributed_set");
/// # }
/// ```
pub struct DSet<K> {
    map: DMap<K, ()>,
}

impl<K> DSet<K>
where
    K: Serialize,
{
    pub(crate) fn new<N>(name: N, cluster: Arc<ClusterRef>) -> Self
    where
        N: Into<String>,
    {
        Self {
            map: DMap::new(name, cluster),
        }
    }

    /// Determine if the set contains `value` but accept a stale result.
    ///
    /// This reads only the local node's copy, which may be arbitrarily behind
    /// the leader. It is equivalent to using [`ReadConsistency::Stale`] with
    /// [`contains_with`](Self::contains_with).
    pub async fn contains_stale<Q>(&self, value: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serialize,
    {
        self.map.contains_key_stale(value).await
    }

    /// Determine if the set contains `value` using the given consistency.
    pub async fn contains_with<Q>(&self, value: &Q, consistency: ReadConsistency) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serialize,
    {
        self.map.contains_key_with(value, consistency).await
    }

    /// Determine if the set contains `value` using strong consistency.
    pub async fn contains<Q>(&self, value: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serialize,
    {
        self.map.contains_key(value).await
    }

    /// Insert `value`, returning `true` if it was not already present.
    pub async fn insert(&self, value: K) -> Result<bool> {
        Ok(self.map.insert(value, ()).await?.is_none())
    }

    /// Remove `value`, returning `true` if it was present.
    pub async fn remove<Q>(&self, value: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serialize,
    {
        Ok(self.map.remove(value).await?.is_some())
    }

    /// Get the number of entries from the local node, accepting a stale result.
    pub async fn len_stale(&self) -> usize {
        self.map.len_stale().await
    }

    /// Get the number of entries using the given consistency.
    pub async fn len_with(&self, consistency: ReadConsistency) -> Result<usize> {
        self.map.len_with(consistency).await
    }

    /// Get the number of entries using strong consistency.
    pub async fn len(&self) -> Result<usize> {
        self.map.len().await
    }

    /// Determine if the local node's copy is empty, accepting a stale result.
    pub async fn is_empty_stale(&self) -> bool {
        self.map.is_empty_stale().await
    }

    /// Determine if the set is empty using the given consistency.
    pub async fn is_empty_with(&self, consistency: ReadConsistency) -> Result<bool> {
        self.map.is_empty_with(consistency).await
    }

    /// Determine if the set is empty using strong consistency.
    pub async fn is_empty(&self) -> Result<bool> {
        self.map.is_empty().await
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::{
        cluster::tests::{single_node_cluster, two_nodes_cluster},
        collections::DSet,
    };

    #[tokio::test]
    async fn insert() -> Result<()> {
        let cluster = single_node_cluster().await?;

        {
            let set: DSet<String> = cluster.get_set("my_set");
            assert!(set.is_empty().await?);
            assert!(set.insert("counter".to_string()).await?);
            assert!(!set.is_empty().await?);
            assert_eq!(set.len().await?, 1);

            assert!(!set.insert("counter".to_string()).await?);
            assert!(!set.is_empty().await?);
            assert_eq!(set.len().await?, 1);
        }

        cluster.shutdown().await?;

        Ok(())
    }

    #[tokio::test]
    async fn contains() -> Result<()> {
        let cluster = single_node_cluster().await?;

        {
            let set: DSet<String> = cluster.get_set("my_set");
            assert!(!set.contains("counter").await?);
            assert!(set.insert("counter".to_string()).await?);
            assert!(set.contains("counter").await?);
        }

        cluster.shutdown().await?;

        Ok(())
    }

    #[tokio::test]
    async fn remove() -> Result<()> {
        let cluster = single_node_cluster().await?;

        {
            let set: DSet<String> = cluster.get_set("my_set");
            assert!(!set.remove("counter").await?);
            assert!(set.insert("counter".to_string()).await?);
            assert!(set.contains("counter").await?);
            assert!(set.remove("counter").await?);
            assert!(set.is_empty().await?);
            assert!(!set.remove("counter").await?);
        }

        cluster.shutdown().await?;

        Ok(())
    }

    #[tokio::test]
    async fn two_nodes() -> Result<()> {
        let (cluster1, cluster2) = two_nodes_cluster().await?;

        {
            let set1: DSet<String> = cluster1.get_set("my_set");
            let set2: DSet<String> = cluster2.get_set("my_set");

            assert!(!set1.contains("counter").await?);
            assert!(!set2.contains("counter").await?);

            assert!(set1.insert("counter".to_string()).await?);
            assert!(set2.contains("counter").await?);
            assert!(!set2.is_empty().await?);
            assert_eq!(set2.len().await?, 1);

            assert!(!set2.insert("counter".to_string()).await?);
            assert!(set1.contains("counter").await?);

            assert!(set2.remove("counter").await?);
            assert!(set1.is_empty().await?);
            assert!(set2.is_empty().await?);
            assert!(!set1.contains("counter").await?);
            assert!(!set2.contains("counter").await?);
        }

        cluster1.shutdown().await?;
        cluster2.shutdown().await?;

        Ok(())
    }
}
