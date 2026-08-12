use std::net::SocketAddr;

use openraft::{
    ChangeMembers,
    error::{ClientWriteError, RaftError},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse, VoteRequest, VoteResponse,
    },
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::raft::{
    TypeConfig,
    node::{Node, NodeId},
};

#[derive(Error, Serialize, Deserialize, Debug)]
#[allow(clippy::large_enum_variant, reason = "RaftError allows this lint too")]
pub(crate) enum AddLearnerError {
    #[error("a node with socket address {addr} (ID: {id}) is already part of the cluster")]
    NodeExists { addr: SocketAddr, id: NodeId },

    #[error(transparent)]
    Raft(#[from] RaftError<NodeId, ClientWriteError<NodeId, Node>>),
}

#[derive(Error, Serialize, Deserialize, Debug)]
pub(crate) enum ResponseError {
    #[error(
        "the message was addressed to the node with ID {target_id} but was \
        received by the node with ID {actual_id}"
    )]
    InvalidNode {
        target_id: NodeId,
        actual_id: NodeId,
    },
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Request {
    pub(crate) target_id: Option<NodeId>,
    pub(crate) body: RequestBody,
}

#[derive(Serialize, Deserialize)]
pub(crate) enum RequestBody {
    AddLearner(NodeId, Node, bool),
    ChangeMembership(ChangeMembers<NodeId, Node>, bool),
    Append(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<NodeId>),
    Insert {
        map: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Get {
        map: String,
        key: Vec<u8>,
    },
    Len {
        map: String,
    },
    Clear {
        map: String,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum Response {
    AddLearner(Result<ClientWriteResponse<TypeConfig>, AddLearnerError>),
    ClientWrite(
        Result<ClientWriteResponse<TypeConfig>, RaftError<NodeId, ClientWriteError<NodeId, Node>>>,
    ),
    Append(Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>),
    Vote(Result<VoteResponse<NodeId>, RaftError<NodeId>>),
    Get(Option<Vec<u8>>),
    Len(Option<usize>),
    Clear,
}
