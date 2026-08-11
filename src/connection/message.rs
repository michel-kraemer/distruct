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

use crate::{
    connection::client::{
        ClearRequest, ClientResponse, GetRequest, InsertRequest, LenRequest, LenResponse,
    },
    raft::{NodeId, TypeConfig, node::Node},
};

#[derive(Error, Serialize, Deserialize, Debug)]
pub enum AddLearnerError {
    #[error("a node with socket address {addr} (ID: {id}) is already part of the cluster")]
    NodeExists { addr: SocketAddr, id: NodeId },

    #[error(transparent)]
    Raft(#[from] RaftError<NodeId, ClientWriteError<NodeId, Node>>),
}

#[derive(Error, Serialize, Deserialize, Debug)]
pub enum ResponseError {
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
    pub target_id: Option<NodeId>,
    pub body: RequestBody,
}

#[derive(Serialize, Deserialize)]
pub enum RequestBody {
    AddLearner(NodeId, Node, bool),
    ChangeMembership(ChangeMembers<NodeId, Node>, bool),
    Append(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<NodeId>),
    Insert(InsertRequest),
    Get(GetRequest),
    Len(LenRequest),
    Clear(ClearRequest),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    AddLearner(Result<ClientWriteResponse<TypeConfig>, AddLearnerError>),
    ClientWrite(
        Result<ClientWriteResponse<TypeConfig>, RaftError<NodeId, ClientWriteError<NodeId, Node>>>,
    ),
    Append(Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>),
    Vote(Result<VoteResponse<NodeId>, RaftError<NodeId>>),
    Get(ClientResponse),
    Len(LenResponse),
    Clear,
}
