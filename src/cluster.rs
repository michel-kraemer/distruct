use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    net::{Ipv6Addr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};

use log::{error, info, warn};
use openraft::{
    ChangeMembers::{AddVoterIds, RemoveNodes, RemoveVoters},
    Raft, ServerState, StoredMembership,
    error::RaftError,
};
use quinn::{
    ClientConfig, Endpoint, ServerConfig,
    crypto::rustls::{QuicClientConfig, QuicServerConfig},
    rustls::{self, RootCertStore},
};
use serde::{Deserialize, Serialize};
use tokio::{
    select,
    sync::{
        broadcast,
        oneshot::{self, Sender},
    },
    task::{JoinError, JoinHandle},
};

use crate::{
    Result,
    collections::dmap::DMap,
    connection::{
        cache::ConnectionCache,
        client::Client,
        message::{Request, RequestBody, Response},
        server::Server,
    },
    error::{ConfigError, RemoteError, SpawnClusterError},
    raft::{
        RaftRequest, TypeConfig,
        heartbeat::HeartbeatMonitor,
        log::LogStorage,
        network::NetworkFactory,
        node::{Node, NodeId},
        state::StateMachine,
    },
    rustls::pki_types::{CertificateDer, PrivateKeyDer},
};

const ALPN_QUIC_CLUSTER: &[u8] = b"cluster";

pub struct ClusterConfig {
    bind_addr: SocketAddr,
    public_addr: Option<SocketAddr>,
    public_server_name: String,
    seed: Option<(SocketAddr, String)>,
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
}

pub struct ClusterConfigBuilder {
    bind_addr: SocketAddr,
    public_addr: Option<SocketAddr>,
    public_server_name: String,
    seed: Option<(SocketAddr, String)>,
}

impl Default for ClusterConfigBuilder {
    fn default() -> Self {
        Self {
            bind_addr: (Ipv6Addr::LOCALHOST, 0).into(),
            public_addr: None,
            public_server_name: "::1".to_string(),
            seed: None,
        }
    }
}

impl ClusterConfigBuilder {
    pub fn with_bind_addr(mut self, addr: SocketAddr) -> Self {
        self.bind_addr = addr;
        self
    }

    pub fn with_public_addr(mut self, addr: SocketAddr, server_name: String) -> Self {
        self.public_addr = Some(addr);
        self.public_server_name = server_name;
        self
    }

    pub fn with_seed(mut self, addr: SocketAddr, server_name: String) -> Self {
        self.seed = Some((addr, server_name));
        self
    }

    pub fn build(
        self,
        certs: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> ClusterConfig {
        ClusterConfig {
            bind_addr: self.bind_addr,
            public_addr: self.public_addr,
            public_server_name: self.public_server_name,
            seed: self.seed,
            certs,
            key,
        }
    }
}

pub struct Cluster {
    raft: Arc<Raft<TypeConfig>>,
    state_machine: Arc<StateMachine>,
    connection_cache: Arc<ConnectionCache>,
    shutdown_tx: oneshot::Sender<()>,
    watch_membership_handle: JoinHandle<()>,
    detect_failures_handle: JoinHandle<()>,
    main_loop_handle: JoinHandle<()>,
}

impl Cluster {
    pub async fn spawn(config: ClusterConfig) -> Result<Self, SpawnClusterError> {
        // create endpoint
        let endpoint = create_endpoint(config.bind_addr, config.certs, config.key)?;

        // run server
        let server = Server::new(endpoint.clone());
        let local_addr = endpoint
            .local_addr()
            .map_err(SpawnClusterError::CreateEndpoint)?;
        info!("Listening on {local_addr} ...");

        // generate unique server ID
        #[cfg(not(test))]
        let server_id = ulid::Ulid::generate();
        #[cfg(test)]
        let server_id = 0;

        // configure Raft
        // TODO make configurable
        let raft_config = Arc::new(openraft::Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        });
        let heartbeat_monitor = Arc::new(HeartbeatMonitor::default());
        let connection_cache = Arc::new(ConnectionCache::new(endpoint));
        let network = NetworkFactory::new(
            Arc::clone(&heartbeat_monitor),
            Arc::clone(&connection_cache),
        );
        let log_storage = LogStorage::default();
        let state_machine = Arc::new(StateMachine::default());
        let raft: Arc<Raft<TypeConfig>> = Arc::new(
            Raft::new(
                server_id,
                raft_config,
                network,
                log_storage,
                Arc::clone(&state_machine),
            )
            .await
            .map_err(RaftError::from)?,
        );

        // configure graceful shutdown
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (shutdown_broadcast_tx, _) = broadcast::channel(1);
        {
            let raft = Arc::clone(&raft);
            let shutdown_broadcast_tx = shutdown_broadcast_tx.clone();
            let connection_cache = Arc::clone(&connection_cache);
            tokio::spawn(async move {
                if let Ok(()) = shutdown_rx.await
                    && let Err(e) =
                        on_graceful_shutdown(raft, shutdown_broadcast_tx, &connection_cache).await
                {
                    error!("Unable to gracefully shutdown node: {e}");
                    std::process::exit(1);
                }
            });
        }

        // watch for membership changes and log them
        let watch_membership_handle = {
            let raft = Arc::clone(&raft);
            let heartbeat_monitor = Arc::clone(&heartbeat_monitor);
            let mut shutdown_broadcast_rx = shutdown_broadcast_tx.subscribe();
            tokio::spawn(async move {
                select! {
                    _ = shutdown_broadcast_rx.recv() => {},
                    _ = watch_membership_changes(heartbeat_monitor, raft) => {},
                }
            })
        };

        // start background task to detect failures and to remove nodes when
        // they've become unavailable
        // TODO the failure detection strategy and what to do in case of
        // failures is application-specific and should be configurable (see
        // Hazelcast failure detectors)
        let detect_failures_handle = {
            let raft = Arc::clone(&raft);
            let shutdown_broadcast_tx = shutdown_broadcast_tx.clone();
            tokio::spawn(async move {
                detect_failures(heartbeat_monitor, raft, shutdown_broadcast_tx).await;
            })
        };

        // main message loop
        let main_loop_handle = {
            let raft = Arc::clone(&raft);
            let state_machine = Arc::clone(&state_machine);
            let shutdown_broadcast_rx = shutdown_broadcast_tx.subscribe();
            tokio::spawn(async move {
                main_loop(server, raft, state_machine, shutdown_broadcast_rx).await;
            })
        };

        let public_addr = config.public_addr.unwrap_or(local_addr);
        let server_node = Node::new(public_addr, config.public_server_name);
        if let Some(seed) = config.seed {
            // join cluster as learner
            let client = Client::new(
                &Node::new(seed.0, seed.1),
                None,
                Arc::clone(&connection_cache),
            )
            .await
            .map_err(SpawnClusterError::Join)?;

            // TODO if response of add_learner or change_membership tells us to
            // forward to leader, then do so
            client
                .add_learner(server_id, server_node, true)
                .await
                .map_err(SpawnClusterError::Join)?;
            info!("Joined cluster as learner");

            client
                .change_membership(AddVoterIds(BTreeSet::from([server_id])), true)
                .await
                .map_err(SpawnClusterError::Join)?;
            info!("Upgraded to voter");

            // wait for the current node to become a follower
            // TODO do we need to make the timeout configurable?
            raft.wait(Some(Duration::from_secs(10)))
                .state(ServerState::Follower, "state")
                .await?;

            client.close();
        } else {
            // initialize single-node cluster
            raft.initialize(BTreeMap::from([(server_id, server_node)]))
                .await?;

            // wait for the current node to become the leader
            // TODO do we need to make the timeout configurable?
            raft.wait(Some(Duration::from_secs(10)))
                .state(ServerState::Leader, "state")
                .await?;
            info!("Node is now leader");
        }

        Ok(Self {
            raft,
            state_machine,
            connection_cache,
            shutdown_tx,
            watch_membership_handle,
            detect_failures_handle,
            main_loop_handle,
        })
    }

    pub async fn shutdown(self) -> Result<(), JoinError> {
        let _ = self.shutdown_tx.send(());
        self.watch_membership_handle.await?;
        self.detect_failures_handle.await?;
        self.main_loop_handle.await?;
        Ok(())
    }

    pub(crate) fn connection_cache(&self) -> &Arc<ConnectionCache> {
        &self.connection_cache
    }

    pub(crate) fn state_machine(&self) -> &Arc<StateMachine> {
        &self.state_machine
    }

    pub fn get_map<'c, K, V, N>(&'c self, name: N) -> DMap<'c, K, V>
    where
        N: Into<String>,
        K: Serialize,
        V: Serialize + for<'a> Deserialize<'a>,
    {
        DMap::new(name, self)
    }

    pub(crate) fn get_leader(&self) -> Option<(NodeId, Node)> {
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow();
        if let Some(leader_id) = metrics.current_leader
            && let Some(leader) = metrics.membership_config.membership().get_node(&leader_id)
        {
            Some((leader_id, leader.clone()))
        } else {
            None
        }
    }
}

fn create_endpoint(
    addr: SocketAddr,
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Result<Endpoint, SpawnClusterError> {
    // create client configuration
    let mut store = RootCertStore::empty();
    for cert in &certs {
        store.add(cert.clone()).map_err(ConfigError::from)?;
    }

    let mut client_crypto = rustls::ClientConfig::builder()
        .with_root_certificates(store)
        .with_no_client_auth();

    client_crypto.alpn_protocols = vec![ALPN_QUIC_CLUSTER.to_vec()];

    let client_config = ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(client_crypto).map_err(ConfigError::from)?,
    ));

    // create server configuration
    let mut server_crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(ConfigError::from)?;

    server_crypto.alpn_protocols = vec![ALPN_QUIC_CLUSTER.to_vec()];

    let mut server_config = ServerConfig::with_crypto(Arc::new(
        QuicServerConfig::try_from(server_crypto).map_err(ConfigError::from)?,
    ));
    let transport_config = Arc::get_mut(&mut server_config.transport).unwrap();
    transport_config.max_concurrent_uni_streams(0_u8.into());

    // create endpoint
    let mut endpoint =
        Endpoint::server(server_config, addr).map_err(SpawnClusterError::CreateEndpoint)?;
    endpoint.set_default_client_config(client_config);

    Ok(endpoint)
}

async fn main_loop(
    mut server: Server,
    raft: Arc<Raft<TypeConfig>>,
    state_machine: Arc<StateMachine>,
    mut shutdown_broadcast_rx: broadcast::Receiver<()>,
) {
    loop {
        select! {
            _ = shutdown_broadcast_rx.recv() => break,
            Some((message, reply)) = server.recv() => {
                handle_message(message, reply, &raft, &state_machine).await;
            }
        }
    }
}

async fn handle_message(
    message: Request,
    reply: Sender<Result<Response, RemoteError>>,
    raft: &Arc<Raft<TypeConfig>>,
    state_machine: &Arc<StateMachine>,
) {
    if let Some(target_id) = message.target_id {
        let current_id = raft.server_metrics().borrow().id;
        if target_id != current_id {
            error!("Reveived message for node {target_id}, but we are node {current_id}");
            if let Err(e) = reply.send(Err(RemoteError::InvalidNode {
                target_id,
                actual_id: current_id,
            })) {
                error!("Unable to send response about invalid target ID to client: {e:?}");
            }
            return;
        }
    }

    let response: Result<Response, RemoteError> = match message.body {
        RequestBody::AddLearner(id, peer, blocking) => {
            info!("Client {id} {} wants to join as learner", peer.addr());
            if let Some(other_node) = raft
                .metrics()
                .borrow()
                .membership_config
                .nodes()
                .find(|(_, m)| m.addr() == peer.addr())
            {
                // a node with this socket address has already joined
                Err(RemoteError::NodeExists {
                    addr: other_node.1.addr(),
                    id: *other_node.0,
                })
            } else {
                raft.add_learner(id, peer, blocking)
                    .await
                    .map(Response::AddLearner)
                    .map_err(|e| e.into())
            }
        }

        RequestBody::ChangeMembership(members, retain) => {
            info!("Client wants to change membership");
            raft.change_membership(members, retain)
                .await
                .map(Response::ClientWrite)
                .map_err(|e| e.into())
        }

        RequestBody::Append(entries) => raft
            .append_entries(entries)
            .await
            .map(Response::Append)
            .map_err(|e| e.into()),

        RequestBody::Vote(rpc) => raft
            .vote(rpc)
            .await
            .map(Response::Vote)
            .map_err(|e| e.into()),

        RequestBody::ContainsKey { map, key } => {
            let value = state_machine.contains_key(&map, &key).await;
            Ok(Response::ContainsKey(value))
        }

        RequestBody::Insert { map, key, value } => {
            let cr = RaftRequest::Insert { map, key, value };
            raft.client_write(cr)
                .await
                .map(Response::ClientWrite)
                .map_err(|e| e.into())
        }

        RequestBody::Get { map, key } => {
            let value = state_machine
                .get_with_lock(&map, &key)
                .await
                .map(|v| v.clone());
            Ok(Response::Get(value))
        }

        RequestBody::Remove { map, key } => {
            let value = state_machine.remove(&map, &key).await;
            Ok(Response::Remove(value))
        }

        RequestBody::Len { map } => {
            let len = state_machine.map_len(&map).await;
            Ok(Response::Len(len))
        }

        RequestBody::Clear { map } => {
            let cr = RaftRequest::Clear { map };
            raft.client_write(cr)
                .await
                .map(Response::ClientWrite)
                .map_err(|e| e.into())
        }
    };

    if let Err(e) = reply.send(response) {
        error!("Unable to send response client: {e:?}");
    }
}

async fn force_shutdown(raft: Arc<Raft<TypeConfig>>, shutdown_broadcast_tx: broadcast::Sender<()>) {
    if let Err(e) = raft.shutdown().await {
        error!("Unable to shutdown raft: {e}");
        std::process::exit(1);
    }
    if let Err(e) = shutdown_broadcast_tx.send(()) {
        error!("Unable to broadcast shutdown signal: {e}");
        std::process::exit(1);
    }
}

async fn on_graceful_shutdown(
    raft: Arc<Raft<TypeConfig>>,
    shutdown_broadcast_tx: broadcast::Sender<()>,
    connection_cache: &Arc<ConnectionCache>,
) -> Result<()> {
    if raft.metrics().borrow().membership_config.nodes().count() > 1 {
        let nodes = BTreeSet::from([raft.server_metrics().borrow().id]);

        if raft.server_metrics().borrow().state.is_leader() {
            raft.change_membership(RemoveVoters(nodes), false)
                .await
                .map_err(RemoteError::from)?;
        } else {
            let leader_id = raft.metrics().borrow().current_leader;
            let leader_node = leader_id.and_then(|leader_id| {
                raft.metrics()
                    .borrow()
                    .membership_config
                    .membership()
                    .get_node(&leader_id)
                    .cloned()
            });
            if let Some(leader_node) = leader_node {
                let client =
                    Client::new(&leader_node, leader_id, Arc::clone(connection_cache)).await?;
                if raft.server_metrics().borrow().state.is_learner() {
                    client.change_membership(RemoveNodes(nodes), false).await?;
                } else {
                    client.change_membership(RemoveVoters(nodes), false).await?;
                }
            }
        }
    }

    force_shutdown(raft, shutdown_broadcast_tx).await;

    Ok(())
}

async fn watch_membership_changes(
    heartbeat_monitor: Arc<HeartbeatMonitor>,
    raft: Arc<Raft<TypeConfig>>,
) {
    let mut metrics_rx = raft.metrics();
    let mut last_membership: Arc<StoredMembership<NodeId, Node>> =
        Arc::clone(&metrics_rx.borrow().membership_config);
    let mut last_msg = String::new();

    while metrics_rx.changed().await.is_ok() {
        let current = Arc::clone(&metrics_rx.borrow().membership_config);
        if current == last_membership {
            continue;
        }

        let leader_id = metrics_rx.borrow().current_leader;
        let mut msg = "Membership changed:\nvoters=[".to_string();

        let mut all_nodes = HashSet::new();
        let mut voters = current.membership().voter_ids().peekable();
        if voters.peek().is_none() {
            msg.push_str("]\n");
        } else {
            while let Some(id) = voters.next() {
                all_nodes.insert(id);
                let n = current.membership().get_node(&id).unwrap();
                msg.push_str(&format!("\n    {id} {}", n.addr()));
                if Some(id) == leader_id {
                    msg.push_str(" [LEADER]");
                }
                if voters.peek().is_some() {
                    msg.push(',');
                }
            }
            msg.push_str("\n]");
        }

        msg.push_str(", learners=[");

        let mut learners = current.membership().learner_ids().peekable();
        if learners.peek().is_none() {
            msg.push_str("]\n");
        } else {
            while let Some(id) = learners.next() {
                all_nodes.insert(id);
                let n = current.membership().get_node(&id).unwrap();
                msg.push_str(&format!("\n    {id} {}", n.addr()));
                if learners.peek().is_some() {
                    msg.push(',');
                }
            }
            msg.push_str("\n]");
        }

        if msg != last_msg {
            info!("{msg}");
            last_msg = msg;
        }

        drop(learners);

        // clean up heartbeat monitor
        heartbeat_monitor.retain(|id| all_nodes.contains(&id));

        last_membership = current;
    }
}

async fn detect_failures(
    heartbeat_monitor: Arc<HeartbeatMonitor>,
    raft: Arc<Raft<TypeConfig>>,
    shutdown_broadcast_tx: broadcast::Sender<()>,
) {
    let mut metrics_rx = raft.metrics();

    let mut interval = tokio::time::interval(Duration::from_secs(1));

    // TODO the deadlines should be much higher and they should be configurable
    const HEARTBEAT_DEADLINE: Duration = Duration::from_secs(10);
    const QUORUM_DEADLINE: Duration = Duration::from_secs(20);

    let mut last_quorum_reached = Instant::now();

    while metrics_rx.changed().await.is_ok() {
        interval.tick().await;

        let mut voters_to_remove = BTreeSet::new();
        let mut learners_to_remove = BTreeSet::new();
        let mut n_voters = 0;
        {
            let metrics = metrics_rx.borrow();
            if metrics.state != ServerState::Leader {
                // only the leader should be able to remove nodes
                continue;
            }

            let current = Arc::clone(&metrics_rx.borrow().membership_config);

            for (node_id, is_voter) in current
                .membership()
                .voter_ids()
                .map(|id| (id, true))
                .chain(current.membership().learner_ids().map(|id| (id, false)))
            {
                if is_voter {
                    n_voters += 1;
                }

                if node_id == metrics.id {
                    // don't monitor ourselves
                    continue;
                }

                let last_seen = heartbeat_monitor.last_seen(node_id);
                if last_seen.elapsed() > HEARTBEAT_DEADLINE {
                    warn!(
                        "Node {node_id} was unreachable for more than {} \
                            seconds. Removing it from the cluster ...",
                        HEARTBEAT_DEADLINE.as_secs()
                    );
                    if is_voter {
                        voters_to_remove.insert(node_id);
                    } else {
                        learners_to_remove.insert(node_id);
                    }
                }
            }
        }

        let available_voters = n_voters - voters_to_remove.len();
        let required_voters_for_quorum = n_voters / 2 + 1;
        let quorum_possible = available_voters >= required_voters_for_quorum;

        if quorum_possible {
            // only try to remove nodes if a quorum is possible - otherwise the
            // proposal will fail anyhow
            last_quorum_reached = Instant::now();

            if !voters_to_remove.is_empty()
                && let Err(e) = raft
                    .change_membership(RemoveVoters(voters_to_remove), false)
                    .await
            {
                error!("Unable to remove voters from cluster: {e}");
            }

            if !learners_to_remove.is_empty()
                && let Err(e) = raft
                    .change_membership(RemoveNodes(learners_to_remove), false)
                    .await
            {
                error!("Unable to remove nodes from cluster: {e}");
            }
        }

        if last_quorum_reached.elapsed() > QUORUM_DEADLINE {
            error!(
                "A quorum could not be reached for more than {} seconds. Shutting down.",
                QUORUM_DEADLINE.as_secs()
            );
            force_shutdown(raft, shutdown_broadcast_tx).await;
            return;
        }
    }
}
