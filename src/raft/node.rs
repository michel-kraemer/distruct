use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    addr: SocketAddr,
    server_name: String,
}

impl Node {
    pub fn new(addr: SocketAddr, server_name: String) -> Self {
        Self { addr, server_name }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn server_name(&self) -> &str {
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
