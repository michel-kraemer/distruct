use openraft::{
    ChangeMembers,
    error::{ClientWriteError as ORClientWriteError, RaftError},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse, VoteRequest, VoteResponse,
    },
};
use quinn::{Connection, ConnectionError, Endpoint, ReadToEndError, WriteError};
use thiserror::Error;

use crate::{
    connection::message::{AddLearnerError, Request, RequestBody, Response, ResponseError},
    raft::{
        TypeConfig,
        node::{Node, NodeId},
    },
};

#[derive(Error, Debug)]
pub(crate) enum ClientConnectError {
    #[error("failed to connect")]
    Connect(#[from] quinn::ConnectError),

    #[error("connection failed")]
    Connection(#[from] quinn::ConnectionError),
}

#[derive(Error, Debug)]
pub(crate) enum ClientRequestError {
    #[error("failed to open stream")]
    Connection(#[from] quinn::ConnectionError),

    #[error("failed to deserialize response")]
    DeserializeResponse(#[source] postcard::Error),

    #[error("failed to read response")]
    ReadToEnd(#[from] ReadToEndError),

    #[error("remote operation failed")]
    Response(#[from] ResponseError),

    #[error("failed to serialize request")]
    SerializeRequest(#[source] postcard::Error),

    #[error("failed to send request")]
    Write(#[from] WriteError),
}

#[derive(Error, Debug)]
pub(crate) enum ClientAddLearnerError {
    #[error("failed to add node as learner")]
    AddLearner(#[from] AddLearnerError),

    #[error("failed to execute request")]
    Request(#[from] ClientRequestError),

    #[error("unknown response")]
    UnknownResponse(Response),
}

#[derive(Error, Debug)]
pub(crate) enum ClientWriteError {
    #[error("remote client write request failed")]
    ClientWrite(#[from] RaftError<NodeId, ORClientWriteError<NodeId, Node>>),

    #[error("failed to execute request")]
    Request(#[from] ClientRequestError),

    #[error("unknown response")]
    UnknownResponse(Response),
}

#[derive(Error, Debug)]
pub(crate) enum ClientRaftError {
    #[error("Raft API error")]
    Raft(#[from] RaftError<NodeId>),

    #[error("failed to execute request")]
    Request(#[from] ClientRequestError),

    #[error("unknown response")]
    UnknownResponse(Response),
}

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
    ) -> Result<Self, ClientConnectError> {
        Ok(Self {
            target_id: node_id,
            conn: endpoint.connect(node.addr(), node.server_name())?.await?,
        })
    }

    async fn request(&self, request: RequestBody) -> Result<Response, ClientRequestError> {
        // open stream
        let (mut send, mut recv) = self.conn.open_bi().await?;

        // send request
        let msg = Request {
            target_id: self.target_id,
            body: request,
        };
        let request = postcard::to_allocvec(&msg).map_err(ClientRequestError::SerializeRequest)?;
        send.write_all(&request).await?;
        send.finish().unwrap();

        // read response
        let resp = recv.read_to_end(usize::MAX).await?;
        let resp: Result<Response, ResponseError> =
            postcard::from_bytes(&resp).map_err(ClientRequestError::DeserializeResponse)?;
        Ok(resp?)
    }

    pub(crate) async fn add_learner(
        &self,
        id: NodeId,
        node: Node,
        blocking: bool,
    ) -> Result<ClientWriteResponse<TypeConfig>, ClientAddLearnerError> {
        let resp = self
            .request(RequestBody::AddLearner(id, node, blocking))
            .await?;
        match resp {
            Response::AddLearner(r) => Ok(r?),
            _ => Err(ClientAddLearnerError::UnknownResponse(resp)),
        }
    }

    pub(crate) async fn change_membership(
        &self,
        members: ChangeMembers<NodeId, Node>,
        retain: bool,
    ) -> Result<ClientWriteResponse<TypeConfig>, ClientWriteError> {
        let resp = self
            .request(RequestBody::ChangeMembership(members, retain))
            .await?;
        match resp {
            Response::ClientWrite(r) => Ok(r?),
            _ => Err(ClientWriteError::UnknownResponse(resp)),
        }
    }

    pub(crate) async fn append_entries(
        &self,
        entries: AppendEntriesRequest<TypeConfig>,
    ) -> Result<AppendEntriesResponse<NodeId>, ClientRaftError> {
        let resp = self.request(RequestBody::Append(entries)).await?;
        match resp {
            Response::Append(r) => Ok(r?),
            _ => Err(ClientRaftError::UnknownResponse(resp)),
        }
    }

    pub(crate) async fn vote(
        &self,
        rpc: VoteRequest<NodeId>,
    ) -> Result<VoteResponse<NodeId>, ClientRaftError> {
        let resp = self.request(RequestBody::Vote(rpc)).await?;
        match resp {
            Response::Vote(r) => Ok(r?),
            _ => Err(ClientRaftError::UnknownResponse(resp)),
        }
    }

    pub(crate) async fn insert<M, KV>(
        &self,
        map: M,
        key: KV,
        value: KV,
    ) -> Result<Option<Vec<u8>>, ClientWriteError>
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
            _ => Err(ClientWriteError::UnknownResponse(resp)),
        }
    }

    pub(crate) async fn get<M, K>(
        &self,
        map: M,
        key: K,
    ) -> Result<Option<Vec<u8>>, ClientWriteError>
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
            _ => Err(ClientWriteError::UnknownResponse(resp)),
        }
    }

    pub(crate) async fn len<M>(&self, map: M) -> Result<Option<usize>, ClientWriteError>
    where
        M: Into<String>,
    {
        let resp = self.request(RequestBody::Len { map: map.into() }).await?;
        match resp {
            Response::Len(response) => Ok(response),
            _ => Err(ClientWriteError::UnknownResponse(resp)),
        }
    }

    pub(crate) async fn clear<M>(&self, map: M) -> Result<(), ClientWriteError>
    where
        M: Into<String>,
    {
        let resp = self.request(RequestBody::Clear { map: map.into() }).await?;
        match resp {
            Response::Clear => Ok(()),
            _ => Err(ClientWriteError::UnknownResponse(resp)),
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
