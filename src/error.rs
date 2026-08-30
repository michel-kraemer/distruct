use std::net::SocketAddr;

use openraft::{
    error::{InitializeError, RaftError},
    metrics::WaitError,
};
use quinn::rustls;
use serde::{Deserialize, Serialize};

use crate::raft::node::{Node, NodeId};

pub type Result<T, E = Error> = core::result::Result<T, E>;

#[allow(non_snake_case)]
#[inline]
pub fn Ok<T>(value: T) -> Result<T> {
    Result::Ok(value)
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("transport error: {0}")]
    Transport(#[from] TransportError),

    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    #[error("remote error: {0}")]
    Remote(#[from] RemoteError),

    #[error("unable to find leader")]
    LeaderNotFound,

    #[error("failed to serialize or deserialize value: {0}")]
    Serialization(#[from] postcard::Error),

    #[error("raft rejected request: {0}")]
    Raft(#[source] Box<RaftError<NodeId>>),

    #[error("failed to check if current node is the leader: {0}")]
    RaftCheckIsLeader(
        #[source] Box<RaftError<NodeId, openraft::error::CheckIsLeaderError<NodeId, Node>>>,
    ),

    #[error("internal error: {0}")]
    Internal(#[from] InternalError),
}

impl From<RaftError<NodeId>> for Error {
    fn from(err: RaftError<NodeId>) -> Self {
        Error::Raft(Box::new(err))
    }
}

impl From<RaftError<NodeId, openraft::error::CheckIsLeaderError<NodeId, Node>>> for Error {
    fn from(err: RaftError<NodeId, openraft::error::CheckIsLeaderError<NodeId, Node>>) -> Self {
        Error::RaftCheckIsLeader(Box::new(err))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SpawnClusterError {
    #[error("configuration error: {0}")]
    Config(#[from] ConfigError),

    #[error("failed to create endpoint: {0}")]
    CreateEndpoint(#[source] std::io::Error),

    #[error("failed to initialize raft: {0}")]
    Initialize(#[source] Box<RaftError<NodeId, InitializeError<NodeId, Node>>>),

    #[error("wait operation failed: {0}")]
    Wait(#[from] WaitError),

    #[error("failed to join cluster: {0}")]
    Join(#[source] Error),
}

impl From<RaftError<NodeId, InitializeError<NodeId, Node>>> for SpawnClusterError {
    fn from(err: RaftError<NodeId, InitializeError<NodeId, Node>>) -> Self {
        SpawnClusterError::Initialize(Box::new(err))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("TLS configuration error: {0}")]
    Rustls(#[from] rustls::Error),

    #[error("no initial cipher suite: {0}")]
    NoInitialCipherSuite(#[from] quinn::crypto::rustls::NoInitialCipherSuite),
}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("failed to connect: {0}")]
    Connect(#[from] quinn::ConnectError),

    #[error("connection lost: {0}")]
    Connection(#[from] quinn::ConnectionError),

    #[error("failed to write to stream: {0}")]
    Write(#[from] quinn::WriteError),

    #[error("failed to read from stream: {0}")]
    Read(#[from] quinn::ReadToEndError),
}

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("failed to deserialize response: {0}")]
    DeserializeResponse(#[source] postcard::Error),

    #[error("unknown response variant")]
    UnknownResponse,
}

#[derive(Debug, thiserror::Error, Serialize, Deserialize)]
pub enum RemoteError {
    #[error("raft rejected write: {0}")]
    RaftWrite(#[source] Box<RaftError<NodeId, openraft::error::ClientWriteError<NodeId, Node>>>),

    #[error("failed to check if current node is the leader: {0}")]
    RaftCheckIsLeader(
        #[source] Box<RaftError<NodeId, openraft::error::CheckIsLeaderError<NodeId, Node>>>,
    ),

    #[error("raft rejected request: {0}")]
    Raft(#[source] Box<RaftError<NodeId>>),

    #[error("a node with socket address {addr} (ID: {id}) is already part of the cluster")]
    NodeExists { id: NodeId, addr: SocketAddr },

    #[error(
        "the message was addressed to the node with ID {target_id} but was \
        received by the node with ID {actual_id}"
    )]
    InvalidNode {
        target_id: NodeId,
        actual_id: NodeId,
    },
}

impl From<RaftError<NodeId, openraft::error::ClientWriteError<NodeId, Node>>> for RemoteError {
    fn from(err: RaftError<NodeId, openraft::error::ClientWriteError<NodeId, Node>>) -> Self {
        RemoteError::RaftWrite(Box::new(err))
    }
}

impl From<RaftError<NodeId, openraft::error::CheckIsLeaderError<NodeId, Node>>> for RemoteError {
    fn from(err: RaftError<NodeId, openraft::error::CheckIsLeaderError<NodeId, Node>>) -> Self {
        RemoteError::RaftCheckIsLeader(Box::new(err))
    }
}

impl From<RaftError<NodeId>> for RemoteError {
    fn from(err: RaftError<NodeId>) -> Self {
        RemoteError::Raft(Box::new(err))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InternalError {
    #[error("request serialization failed: {0}")]
    SerializeRequest(#[source] postcard::Error),
}
