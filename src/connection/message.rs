use openraft::{
    ChangeMembers,
    error::{ClientWriteError, RaftError},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse, VoteRequest, VoteResponse,
    },
};
use serde::{Deserialize, Serialize};

use crate::{
    connection::client::{GetRequest, InsertRequest},
    raft::{NodeId, TypeConfig, node::Node},
};

#[derive(Serialize, Deserialize)]
pub enum Request {
    AddLearner(NodeId, Node, bool),
    ChangeMembership(ChangeMembers<NodeId, Node>, bool),
    Append(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<NodeId>),
    Insert(InsertRequest),
    Get(GetRequest),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    ClientWrite(
        Result<ClientWriteResponse<TypeConfig>, RaftError<NodeId, ClientWriteError<NodeId, Node>>>,
    ),
    Append(Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>),
    Vote(Result<VoteResponse<NodeId>, RaftError<NodeId>>),
    Get(Option<Vec<u8>>),
}
