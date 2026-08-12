use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use anyhow::{Context, Result};
use clap::Parser;
use distruct::rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use distruct::{Cluster, ClusterConfigBuilder, collections::dmap::DMap};
use log::error;
use tokio::{select, signal, sync::oneshot};
use tracing_subscriber::EnvFilter;

const DEFAULT_PORT: u16 = 35000;

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    bind_addr: IpAddr,

    #[arg(long, default_value_t = DEFAULT_PORT)]
    bind_port: u16,

    #[arg(long)]
    public_addr: String,

    #[arg(long)]
    public_port: u16,

    #[arg(long)]
    seed_addr: Option<String>,

    #[arg(long)]
    seed_port: Option<u16>,
}

async fn resolve(addr: &str, port: u16) -> Result<SocketAddr> {
    tokio::net::lookup_host((addr, port))
        .await
        .with_context(|| format!("failed to resolve seed address: {addr}:{port}"))?
        .next()
        .with_context(|| format!("no addresses found for seed: {addr}:{port}"))
}

#[tokio::main]
async fn main() -> Result<()> {
    // initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    // resolve public addr
    let public_addr = resolve(&cli.public_addr, cli.public_port).await?;

    let mut cluster_config_builder = ClusterConfigBuilder::default()
        .with_bind_addr((cli.bind_addr, cli.bind_port).into())
        .with_public_addr(public_addr, cli.public_addr);

    // add seed if there is any
    let mut has_seed = false;
    if let Some(seed_addr) = cli.seed_addr {
        let seed_port = cli.seed_port.unwrap_or(DEFAULT_PORT);
        let resolved_seed_addr = resolve(&seed_addr, seed_port).await?;
        cluster_config_builder = cluster_config_builder.with_seed(resolved_seed_addr, seed_addr);
        has_seed = true;
    }

    // load server certificate and private key
    let certs: Vec<CertificateDer> = CertificateDer::pem_file_iter("cert.pem")
        .context("failed to read certificate chain file")?
        .map(|e| Ok(e?))
        .collect::<Result<_>>()
        .context("invalid PEM-encoded certificate")?;
    let key = PrivateKeyDer::from_pem_file("key.pem").context("failed to read private key file")?;

    let cluster_config = cluster_config_builder.build(certs, key);

    let cluster = Cluster::spawn(cluster_config).await?;

    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::spawn(async move {
        select! {
            _ = ctrl_c => shutdown_tx.send(()),
            _ = terminate => shutdown_tx.send(()),
        }
    });

    let map: DMap<String, String> = cluster.get_map("my_map");
    if has_seed {
        let v = map.get("Hello").await?;
        if let Some(v) = v {
            println!("FOUND VALUE: {v} {}", map.len().await?);
        } else {
            println!("INSERT NEW VALUE");
            map.insert("Hello".to_string(), "World".to_string()).await?;
        }
    }

    let mut interval = tokio::time::interval(Duration::from_secs(1));
    let mut step = 0;
    loop {
        select! {
            _ = &mut shutdown_rx => {
                break;
            },

            _ = interval.tick() => {
                let v = map.get_stale("Hello").await?;
                println!("CURRENT VALUE: {v:?} {}", map.len_stale().await);
                step += 1;
                if step == 10 && let Err(e) = map.insert("Hello".to_string(), "World".to_string()).await.context("failed to insert value") {
                        error!("failed to insert value: {e:?}");
                }
            }
        }
    }

    // shutdown_rx.await?;

    cluster.shutdown().await?;

    Ok(())
}
