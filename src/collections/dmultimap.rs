use std::{borrow::Borrow, marker::PhantomData, sync::Arc};

use serde::{Deserialize, Serialize};

use super::{ReadConsistency, should_read_stale};
use crate::{Error, Result, cluster::Cluster, connection::client::Client};

pub struct DMultiMap<'c, K, V> {
    name: String,
    cluster: &'c Cluster,
    _marker: PhantomData<(K, V)>,
}

impl<'c, K, V> DMultiMap<'c, K, V>
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

    pub async fn insert(&self, k: K, v: V) -> Result<bool> {
        self.client_to_leader()
            .await?
            .insert_multi(
                &self.name,
                postcard::to_allocvec(&k)?,
                postcard::to_allocvec(&v)?,
            )
            .await
    }

    pub async fn get_stale<Q>(&self, k: &Q) -> Result<Vec<V>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serialize,
    {
        let key = postcard::to_allocvec(k)?;
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

    pub async fn get_with<Q>(&self, k: &Q, consistency: ReadConsistency) -> Result<Vec<V>>
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
                .get_multi(&self.name, postcard::to_allocvec(k)?, consistency)
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

    pub async fn get<Q>(&self, k: &Q) -> Result<Vec<V>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serialize,
    {
        self.get_with(k, ReadConsistency::ReadIndex).await
    }

    pub async fn remove<Q>(&self, k: &Q, v: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        V: Borrow<Q>,
        Q: ?Sized + Serialize,
    {
        self.client_to_leader()
            .await?
            .remove_multi(
                &self.name,
                postcard::to_allocvec(k)?,
                postcard::to_allocvec(v)?,
            )
            .await
    }
}
