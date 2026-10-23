//! Raft-based cluster coordination and strongly-consistent distributed data
//! structures.
//!
//! This crate provides a set of distributed data structures that can be used to
//! build applications that require strong consistency. The data structures are
//! built on top of a Raft-based cluster coordination system that ensures that
//! all nodes in the cluster have a consistent view of the data.
//!
//! # Spawning a cluster node
//!
//! Cluster nodes communicate with each other over QUIC. Each node is bound to a
//! specific IP address and port. In addition, a public address can be
//! configured to announce the node in the cluster, so other nodes can reach it.
//!
//! Before spawning a node, configure it as follows:
//!
//! ```no_run
//! use distruct::{
//!     ClusterConfigBuilder,
//!     rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject as _},
//! };
//! use std::net::{Ipv4Addr, Ipv6Addr};
//!
//! # fn config() -> Result<(), Box<dyn std::error::Error>> {
//! // set address to bind to and the node's public address
//! let mut cluster_config_builder = ClusterConfigBuilder::default()
//!     .with_bind_addr((Ipv6Addr::LOCALHOST, 35000).into())
//!     .with_public_addr((Ipv4Addr::new(192, 0, 2, 0), 80).into(), "example.com");
//!
//! // Optionally, add the address of another node of an existing cluster to
//! // connect to. If you don't set a seed, a new single-node cluster will be
//! // spawned.
//! cluster_config_builder =
//!     cluster_config_builder.with_seed((Ipv4Addr::new(192, 0, 2, 1), 80).into(), "example.com");
//!
//! // load server certificate and private key
//! let certs: Vec<CertificateDer> =
//!     CertificateDer::pem_file_iter("cert.pem")?.collect::<Result<_, _>>()?;
//! let key = PrivateKeyDer::from_pem_file("key.pem")?;
//!
//! // build the configuration
//! let config = cluster_config_builder.build(certs, key);
//! # Ok(())
//! # }
//! ```
//!
//! Then, spawn the cluster:
//!
//! ```no_run
//! use distruct::Cluster;
//!
//! # async fn spawn(config: distruct::ClusterConfig) -> Result<(), Box<dyn std::error::Error>> {
//! let cluster = Cluster::spawn(config).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Obtaining a distributed data structure
//!
//! After spawning the cluster, you may obtain a distributed data structure such
//! as a map as follows:
//!
//! ```no_run
//! use distruct::collections::DMap;
//!
//! # async fn map(cluster: distruct::Cluster) -> Result<(), Box<dyn std::error::Error>> {
//! let map: DMap<String, String> = cluster.get_map("my_map");
//! map.insert("Hello".to_string(), "World".to_string()).await?;
//! # Ok(())
//! # }
//! ```
//!
//! Keys and values must implement serde's `Serialize` and `Deserialize` traits:
//!
//! ```no_run
//! use serde::{Deserialize, Serialize};
//! # use distruct::collections::DMap;
//!
//! #[derive(Deserialize, Serialize)]
//! struct MyValue {
//!     message: String,
//! }
//!
//! # async fn map(cluster: distruct::Cluster) -> Result<(), Box<dyn std::error::Error>> {
//! let map: DMap<String, MyValue> = cluster.get_map("my_map");
//! map.insert(
//!     "Hello".to_string(),
//!     MyValue {
//!         message: "World".to_string(),
//!     },
//! )
//! .await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Gracefully shutting down the cluster node
//!
//! If you want to remove a node from the cluster gracefully, use
//! [`Cluster::shutdown`]. The method will fail if there are still references to
//! the cluster node instance somewhere in your code, so make sure you first
//! drop all resources such as distributed maps.
//!
//! ```no_run
//! # async fn shutdown(cluster: distruct::Cluster, map: distruct::collections::DMap<String, String>) -> Result<(), Box<dyn std::error::Error>> {
//! // make sure there are no shared references to the cluster anymore
//! drop(map);
//!
//! // gracefully shutdown the cluster
//! cluster.shutdown().await?;
//! # Ok(())
//! # }
//! ```
mod cluster;
pub mod collections;
mod connection;
pub mod error;
mod raft;

pub use cluster::{Cluster, ClusterConfig, ClusterConfigBuilder};
pub use quinn::rustls;
