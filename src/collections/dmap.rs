use std::{borrow::Borrow, marker::PhantomData};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::{
    cluster::Cluster,
    connection::client::{ClearRequest, GetRequest, InsertRequest, LenRequest},
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

    pub async fn insert(&self, k: K, v: V) -> Result<Option<V>> {
        // TODO redirect to another node and store new leader if necessary

        let (leader_id, leader) = self.cluster.get_leader().context("unable to find leader")?;
        let client = self
            .cluster
            .pool()
            .connect(&leader, Some(leader_id))
            .await?;
        let result = client
            .insert(InsertRequest {
                map: self.name.clone(),
                key: postcard::to_allocvec(&k)?,
                value: postcard::to_allocvec(&v)?,
            })
            .await?;

        Ok(result
            .value
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
        // TODO redirect to another node and store new leader if necessary

        let (leader_id, leader) = self.cluster.get_leader().context("unable to find leader")?;
        let client = self
            .cluster
            .pool()
            .connect(&leader, Some(leader_id))
            .await?;

        let result = client
            .get(GetRequest {
                map: self.name.clone(),
                key: postcard::to_allocvec(k)?,
            })
            .await?;

        Ok(result
            .value
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
        // TODO redirect to another node and store new leader if necessary

        let (leader_id, leader) = self.cluster.get_leader().context("unable to find leader")?;
        let client = self
            .cluster
            .pool()
            .connect(&leader, Some(leader_id))
            .await?;

        let result = client
            .len(LenRequest {
                map: self.name.clone(),
            })
            .await?;

        Ok(result.len.unwrap_or_default())
    }

    pub async fn is_empty_stale(&self) -> bool {
        self.len_stale().await == 0
    }

    pub async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    pub async fn clear(&self) -> Result<()> {
        // TODO redirect to another node and store new leader if necessary

        let (leader_id, leader) = self.cluster.get_leader().context("unable to find leader")?;
        let client = self
            .cluster
            .pool()
            .connect(&leader, Some(leader_id))
            .await?;

        client
            .clear(ClearRequest {
                map: self.name.clone(),
            })
            .await?;

        Ok(())
    }
}
