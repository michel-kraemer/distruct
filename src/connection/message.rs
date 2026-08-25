use openraft::{
    ChangeMembers,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse, VoteRequest, VoteResponse,
    },
};
use serde::{Deserialize, Serialize};

use crate::{
    Result,
    error::RemoteError,
    raft::{
        TypeConfig,
        node::{Node, NodeId},
    },
};

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
    ContainsKey {
        map: String,
        key: Vec<u8>,
    },
    Insert {
        map: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Get {
        map: String,
        key: Vec<u8>,
    },
    Remove {
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
    AddLearner(Result<ClientWriteResponse<TypeConfig>, RemoteError>),
    ClientWrite(Result<ClientWriteResponse<TypeConfig>, RemoteError>),
    Append(Result<AppendEntriesResponse<NodeId>, RemoteError>),
    Vote(Result<VoteResponse<NodeId>, RemoteError>),
    ContainsKey(bool),
    Get(Option<Vec<u8>>),
    Remove(Option<Vec<u8>>),
    Len(Option<usize>),
    Clear,
}
