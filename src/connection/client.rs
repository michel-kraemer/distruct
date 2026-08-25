use openraft::{
    ChangeMembers,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse, VoteRequest, VoteResponse,
    },
};
use quinn::{Connection, ConnectionError, Endpoint};

use crate::{
    Result,
    connection::message::{Request, RequestBody, Response},
    error::{InternalError, ProtocolError, RemoteError, TransportError},
    raft::{
        TypeConfig,
        node::{Node, NodeId},
    },
};

#[derive(Clone)]
pub(crate) struct Client {
    conn: Connection,
    target_id: Option<NodeId>,
}

impl Client {
    pub(super) async fn new(
        node: &Node,
        node_id: Option<NodeId>,
        endpoint: &Endpoint,
    ) -> Result<Self> {
        Ok(Self {
            target_id: node_id,
            conn: endpoint
                .connect(node.addr(), node.server_name())
                .map_err(TransportError::from)?
                .await
                .map_err(TransportError::from)?,
        })
    }

    async fn request(&self, request: RequestBody) -> Result<Response> {
        // open stream
        let (mut send, mut recv) = self.conn.open_bi().await.map_err(TransportError::from)?;

        // send request
        let msg = Request {
            target_id: self.target_id,
            body: request,
        };
        let request = postcard::to_allocvec(&msg).map_err(InternalError::SerializeRequest)?;
        send.write_all(&request)
            .await
            .map_err(TransportError::from)?;
        send.finish().unwrap();

        // read response
        let resp = recv
            .read_to_end(usize::MAX)
            .await
            .map_err(TransportError::from)?;
        let resp: Result<Response, RemoteError> =
            postcard::from_bytes(&resp).map_err(ProtocolError::DeserializeResponse)?;
        Ok(resp?)
    }

    pub(crate) async fn add_learner(
        &self,
        id: NodeId,
        node: Node,
        blocking: bool,
    ) -> Result<ClientWriteResponse<TypeConfig>> {
        let resp = self
            .request(RequestBody::AddLearner(id, node, blocking))
            .await?;
        match resp {
            Response::AddLearner(r) => Ok(r?),
            _ => Err(ProtocolError::UnknownResponse)?,
        }
    }

    pub(crate) async fn change_membership(
        &self,
        members: ChangeMembers<NodeId, Node>,
        retain: bool,
    ) -> Result<ClientWriteResponse<TypeConfig>> {
        let resp = self
            .request(RequestBody::ChangeMembership(members, retain))
            .await?;
        match resp {
            Response::ClientWrite(r) => Ok(r?),
            _ => Err(ProtocolError::UnknownResponse)?,
        }
    }

    pub(crate) async fn append_entries(
        &self,
        entries: AppendEntriesRequest<TypeConfig>,
    ) -> Result<AppendEntriesResponse<NodeId>> {
        let resp = self.request(RequestBody::Append(entries)).await?;
        match resp {
            Response::Append(r) => Ok(r?),
            _ => Err(ProtocolError::UnknownResponse)?,
        }
    }

    pub(crate) async fn vote(&self, rpc: VoteRequest<NodeId>) -> Result<VoteResponse<NodeId>> {
        let resp = self.request(RequestBody::Vote(rpc)).await?;
        match resp {
            Response::Vote(r) => Ok(r?),
            _ => Err(ProtocolError::UnknownResponse)?,
        }
    }

    pub(crate) async fn contains_key<M, K>(&self, map: M, key: K) -> Result<bool>
    where
        M: Into<String>,
        K: Into<Vec<u8>>,
    {
        let resp = self
            .request(RequestBody::ContainsKey {
                map: map.into(),
                key: key.into(),
            })
            .await?;
        match resp {
            Response::ContainsKey(response) => Ok(response),
            _ => Err(ProtocolError::UnknownResponse)?,
        }
    }

    pub(crate) async fn insert<M, KV>(&self, map: M, key: KV, value: KV) -> Result<Option<Vec<u8>>>
    where
        M: Into<String>,
        KV: Into<Vec<u8>>,
    {
        let resp = self
            .request(RequestBody::Insert {
                map: map.into(),
                key: key.into(),
                value: value.into(),
            })
            .await?;
        match resp {
            Response::ClientWrite(r) => {
                let r = r?;
                Ok(r.data.value)
            }
            _ => Err(ProtocolError::UnknownResponse)?,
        }
    }

    pub(crate) async fn get<M, K>(&self, map: M, key: K) -> Result<Option<Vec<u8>>>
    where
        M: Into<String>,
        K: Into<Vec<u8>>,
    {
        let resp = self
            .request(RequestBody::Get {
                map: map.into(),
                key: key.into(),
            })
            .await?;
        match resp {
            Response::Get(response) => Ok(response),
            _ => Err(ProtocolError::UnknownResponse)?,
        }
    }

    pub(crate) async fn remove<M, K>(&self, map: M, key: K) -> Result<Option<Vec<u8>>>
    where
        M: Into<String>,
        K: Into<Vec<u8>>,
    {
        let resp = self
            .request(RequestBody::Remove {
                map: map.into(),
                key: key.into(),
            })
            .await?;
        match resp {
            Response::Remove(response) => Ok(response),
            _ => Err(ProtocolError::UnknownResponse)?,
        }
    }

    pub(crate) async fn len<M>(&self, map: M) -> Result<Option<usize>>
    where
        M: Into<String>,
    {
        let resp = self.request(RequestBody::Len { map: map.into() }).await?;
        match resp {
            Response::Len(response) => Ok(response),
            _ => Err(ProtocolError::UnknownResponse)?,
        }
    }

    pub(crate) async fn clear<M>(&self, map: M) -> Result<()>
    where
        M: Into<String>,
    {
        let resp = self.request(RequestBody::Clear { map: map.into() }).await?;
        match resp {
            Response::Clear => Ok(()),
            _ => Err(ProtocolError::UnknownResponse)?,
        }
    }

    pub(super) fn is_open(&self) -> bool {
        self.conn.close_reason().is_none()
    }

    /// Wait for the client to be closed for any reason
    pub(super) async fn closed(&self) -> ConnectionError {
        self.conn.closed().await
    }

    pub(crate) fn close(&self) {
        self.conn.close(0u32.into(), b"done");
    }
}
