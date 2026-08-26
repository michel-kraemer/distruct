use std::{
    fmt::{Display, Formatter},
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

use serde::{Deserialize, Serialize};

#[cfg(not(test))]
pub type NodeId = ulid::Ulid;
#[cfg(test)]
pub type NodeId = u64;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    addr: SocketAddr,
    server_name: String,
}

impl Node {
    pub(crate) fn new(addr: SocketAddr, server_name: String) -> Self {
        Self { addr, server_name }
    }

    pub(crate) fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub(crate) fn server_name(&self) -> &str {
        &self.server_name
    }
}

impl Default for Node {
    fn default() -> Self {
        Self {
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0),
            server_name: "localhost".to_string(),
        }
    }
}

impl Display for Node {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.server_name, self.addr)
    }
}
