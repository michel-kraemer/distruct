use std::{borrow::Borrow, marker::PhantomData, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{Error, Result, cluster::Cluster, connection::client::Client};

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

    pub async fn contains_key<Q>(&self, k: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serialize,
    {
        self.client_to_leader()
            .await?
            .contains_key(&self.name, postcard::to_allocvec(k)?)
            .await
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

    pub async fn get<Q>(&self, k: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Serialize,
    {
        Ok(self
            .client_to_leader()
            .await?
            .get(&self.name, postcard::to_allocvec(k)?)
            .await?
            .map(|value| postcard::from_bytes(&value))
            .transpose()?)
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

    pub async fn len(&self) -> Result<usize> {
        let result = self.client_to_leader().await?.len(&self.name).await?;
        Ok(result.unwrap_or_default())
    }

    pub async fn is_empty_stale(&self) -> bool {
        self.len_stale().await == 0
    }

    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    pub async fn clear(&self) -> Result<()> {
        self.client_to_leader().await?.clear(&self.name).await
    }
}
