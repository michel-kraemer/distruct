use openraft::{
    ChangeMembers,
    error::{ClientWriteError as ORClientWriteError, RaftError},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse, VoteRequest, VoteResponse,
    },
};
use quinn::{Connection, ConnectionError, Endpoint, ReadToEndError, WriteError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    connection::message::{AddLearnerError, Request, RequestBody, Response, ResponseError},
    raft::{NodeId, TypeConfig, node::Node},
};

#[derive(Serialize, Deserialize)]
pub struct InsertRequest {
    pub map: String,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub struct GetRequest {
    pub map: String,
    pub key: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub struct LenRequest {
    pub map: String,
}

#[derive(Serialize, Deserialize)]
pub struct ClearRequest {
    pub map: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientResponse {
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LenResponse {
    pub len: Option<usize>,
}

#[derive(Error, Debug)]
pub enum ClientConnectError {
    #[error("failed to connect")]
    Connect(#[from] quinn::ConnectError),

    #[error("connection failed")]
    Connection(#[from] quinn::ConnectionError),
}

#[derive(Error, Debug)]
pub enum ClientRequestError {
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
pub enum ClientAddLearnerError {
    #[error("failed to add node as learner")]
    AddLearner(#[from] AddLearnerError),

    #[error("failed to execute request")]
    Request(#[from] ClientRequestError),

    #[error("unknown response")]
    UnknownResponse(Response),
}

#[derive(Error, Debug)]
pub enum ClientWriteError {
    #[error("remote client write request failed")]
    ClientWrite(#[from] RaftError<NodeId, ORClientWriteError<NodeId, Node>>),

    #[error("failed to execute request")]
    Request(#[from] ClientRequestError),

    #[error("unknown response")]
    UnknownResponse(Response),
}

#[derive(Error, Debug)]
pub enum ClientRaftError {
    #[error("Raft API error")]
    Raft(#[from] RaftError<NodeId>),

    #[error("failed to execute request")]
    Request(#[from] ClientRequestError),

    #[error("unknown response")]
    UnknownResponse(Response),
}

#[derive(Clone)]
pub struct Client {
    conn: Connection,
    target_id: Option<NodeId>,
}

impl Client {
    pub async fn new(
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

    pub async fn add_learner(
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

    pub async fn change_membership(
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

    pub async fn append_entries(
        &self,
        entries: AppendEntriesRequest<TypeConfig>,
    ) -> Result<AppendEntriesResponse<NodeId>, ClientRaftError> {
        let resp = self.request(RequestBody::Append(entries)).await?;
        match resp {
            Response::Append(r) => Ok(r?),
            _ => Err(ClientRaftError::UnknownResponse(resp)),
        }
    }

    pub async fn vote(
        &self,
        rpc: VoteRequest<NodeId>,
    ) -> Result<VoteResponse<NodeId>, ClientRaftError> {
        let resp = self.request(RequestBody::Vote(rpc)).await?;
        match resp {
            Response::Vote(r) => Ok(r?),
            _ => Err(ClientRaftError::UnknownResponse(resp)),
        }
    }

    pub async fn insert(&self, request: InsertRequest) -> Result<ClientResponse, ClientWriteError> {
        let resp = self.request(RequestBody::Insert(request)).await?;
        match resp {
            Response::ClientWrite(r) => {
                let r = r?;
                Ok(ClientResponse {
                    value: r.data.value,
                })
            }
            _ => Err(ClientWriteError::UnknownResponse(resp)),
        }
    }

    pub async fn get(&self, request: GetRequest) -> Result<ClientResponse, ClientWriteError> {
        let resp = self.request(RequestBody::Get(request)).await?;
        match resp {
            Response::Get(response) => Ok(response),
            _ => Err(ClientWriteError::UnknownResponse(resp)),
        }
    }

    pub async fn len(&self, request: LenRequest) -> Result<LenResponse, ClientWriteError> {
        let resp = self.request(RequestBody::Len(request)).await?;
        match resp {
            Response::Len(response) => Ok(response),
            _ => Err(ClientWriteError::UnknownResponse(resp)),
        }
    }

    pub async fn clear(&self, request: ClearRequest) -> Result<(), ClientWriteError> {
        let resp = self.request(RequestBody::Clear(request)).await?;
        match resp {
            Response::Clear => Ok(()),
            _ => Err(ClientWriteError::UnknownResponse(resp)),
        }
    }

    pub fn is_open(&self) -> bool {
        self.conn.close_reason().is_none()
    }

    /// Wait for the client to be closed for any reason
    pub async fn closed(&self) -> ConnectionError {
        self.conn.closed().await
    }

    pub fn close(&self) {
        self.conn.close(0u32.into(), b"done");
    }
}
