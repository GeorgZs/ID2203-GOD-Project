use futures::{SinkExt, StreamExt};
use log::*;
use god_db::common::{
    ds::{ClientId, NodeId},
    messages::*,
    utils::*,
};
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::net::SocketAddr;
use std::sync::{Arc};
use std::time::Duration;
use tokio::{
    io::AsyncWriteExt,
    sync::mpsc::{Sender, UnboundedSender},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::Receiver,
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

pub struct CliNetwork {
    id: NodeId,
    batch_size: usize,
    cancel_token: CancellationToken,
    cli_client_connections: Arc<Mutex<HashMap<RequestIdentifier, ClientConnection<RequestIdentifier>>>>,
    cli_client_message_sender: Sender<(RequestIdentifier, ClientMessage)>,
    pub cli_client_messages: Arc<Mutex<Receiver<(RequestIdentifier, ClientMessage)>>>,
}

impl CliNetwork {
    pub fn new(id: NodeId, batch_size: usize) -> Self {
        let (cli_client_message_sender, cli_client_messages) = tokio::sync::mpsc::channel(batch_size);
        CliNetwork {
            id,
            batch_size,
            cancel_token: CancellationToken::new(),
            cli_client_connections: Arc::new(Mutex::new(HashMap::new())),
            cli_client_message_sender,
            cli_client_messages: Arc::new(Mutex::new(cli_client_messages)),
        }
    }

    pub async fn listen_to_connections(&self) {
        let (connection_sink, mut connection_source) = mpsc::channel(30);
        let _ = self.spawn_connection_listener(connection_sink.clone());
        while let Some(new_connection) = connection_source.recv().await {
            match new_connection {
                NewConnection::ToPeer(_) => {
                    // ignore
                }
                NewConnection::ToClient(_) => {
                    // ignore
                }
                NewConnection::ToCliClient(connection) => {
                    let cli_conns = Arc::clone(&self.cli_client_connections);
                    let mut connections = cli_conns.lock().await;
                    connections.insert(connection.client_id.clone(), connection);
                }
            }
        }
    }
    #[allow(dead_code)]
    pub async fn send_to_cli_client(&self, to: RequestIdentifier, msg: ServerMessage) {
        let cli_conns = Arc::clone(&self.cli_client_connections);
        let mut connections = cli_conns.lock().await;
        match connections.get_mut(&to) {
            Some(connection) => {
                info!("Sending response to cli client {}", to);
                if let Err(err) = connection.send(msg) {
                    warn!("Couldn't send msg to client {to}: {err}");
                    connections.remove(&to);
                }
            }
            None => warn!("Not connected to client {to}"),
        }
    }

    fn spawn_connection_listener(
        &self,
        connection_sender: Sender<NewConnection>,
    ) -> tokio::task::JoinHandle<()> {
        let port = 8000 + self.id as u16;
        let listening_address = SocketAddr::from(([0, 0, 0, 0], port));
        let cli_client_sender = self.cli_client_message_sender.clone();
        let batch_size = self.batch_size;
        let cancel_token = self.cancel_token.clone();
        tokio::spawn(async move {
            let listener = TcpListener::bind(listening_address).await.unwrap();
            loop {
                match listener.accept().await {
                    Ok((tcp_stream, socket_addr)) => {
                        info!("New connection from {socket_addr}");
                        tcp_stream.set_nodelay(true).unwrap();
                        tokio::spawn(Network::handle_incoming_connection(
                            tcp_stream,
                            Some(cli_client_sender.clone()),
                            None,
                            None,
                            connection_sender.clone(),
                            None,
                            batch_size,
                            cancel_token.clone(),
                        ));
                    }
                    Err(e) => error!("Error listening for new connection: {:?}", e),
                }
            }
        })
    }
}

pub struct Network {
    cluster_name: String,
    id: NodeId,
    peers: Vec<NodeId>,
    is_local: bool,
    peer_connections: Arc<Mutex<Vec<Option<PeerConnection>>>>,
    client_connections: Arc<Mutex<HashMap<ClientId, ClientConnection<ClientId>>>>,
    max_client_id: Arc<Mutex<ClientId>>,
    batch_size: usize,
    client_message_sender: Sender<(ClientId, ClientMessage)>,
    pub cluster_message_sender: Sender<(NodeId, ClusterMessage)>,
    pub cluster_messages: Arc<Mutex<Receiver<(NodeId, ClusterMessage)>>>,
    pub client_messages: Arc<Mutex<Receiver<(ClientId, ClientMessage)>>>,
    cancel_token: CancellationToken,
}

impl Network {
    // Creates a new network with connections other server nodes in the cluster and any clients.
    // Waits until connections to all servers and clients are established before resolving.
    pub async fn new(
        cluster_name: String,
        id: NodeId,
        nodes: Vec<NodeId>,
        num_clients: usize,
        local_deployment: bool,
        batch_size: usize,
    ) -> Self {
        let peers: Vec<u64> = nodes.iter().cloned().filter(|node| *node != id).collect();
        let mut cluster_connections = vec![];
        cluster_connections.resize_with(peers.len(), Default::default);
        let (cluster_message_sender, cluster_messages) = tokio::sync::mpsc::channel(batch_size);
        let (client_message_sender, client_messages) = tokio::sync::mpsc::channel(batch_size);
        let mut network = Self {
            cluster_name,
            is_local: local_deployment,
            id,
            peers,
            peer_connections: Arc::new(Mutex::new(cluster_connections)),
            client_connections: Arc::new(Mutex::new(HashMap::new())),
            max_client_id: Arc::new(Mutex::new(0)),
            batch_size,
            client_message_sender,
            cluster_message_sender,
            cluster_messages: Arc::new(Mutex::new(cluster_messages)),
            client_messages: Arc::new(Mutex::new(client_messages)),
            cancel_token: CancellationToken::new(),
        };
        network.initialize_connections(num_clients).await;
        network
    }

    async fn initialize_connections(&mut self, num_clients: usize) {
        let (connection_sink, mut connection_source) = mpsc::channel(30);
        let listener_handle = self.spawn_connection_listener(connection_sink.clone());
        self.spawn_peer_connectors(connection_sink.clone());
        while let Some(new_connection) = connection_source.recv().await {
            match new_connection {
                NewConnection::ToPeer(connection) => {
                    let peer_idx = self.cluster_id_to_idx(connection.peer_id).unwrap();
                    let peer_conns_clone = Arc::clone(&self.peer_connections);
                    let mut peer_connections = peer_conns_clone.lock().await;
                    peer_connections[peer_idx] = Some(connection);
                }
                NewConnection::ToClient(connection) => {
                    let conns = Arc::clone(&self.client_connections);
                    let mut connections = conns.lock().await;
                    let _ = connections
                        .insert(connection.client_id, connection);
                }
                NewConnection::ToCliClient(_) => {
                    // ignore
                }
            }
            let conns = Arc::clone(&self.client_connections);
            let connections = conns.lock().await;
            let all_clients_connected = connections.len() >= num_clients;
            let peer_conns_clone = Arc::clone(&self.peer_connections);
            let peer_connections = peer_conns_clone.lock().await;
            let all_cluster_connected = peer_connections.iter().all(|c| c.is_some());
            if all_clients_connected && all_cluster_connected {
                listener_handle.abort();
                break;
            }
        }
    }

    fn spawn_connection_listener(
        &self,
        connection_sender: Sender<NewConnection>,
    ) -> tokio::task::JoinHandle<()> {
        let port = 8000 + self.id as u16;
        let listening_address = SocketAddr::from(([0, 0, 0, 0], port));
        let client_sender = self.client_message_sender.clone();
        let cluster_sender = self.cluster_message_sender.clone();
        let max_client_id_handle = self.max_client_id.clone();
        let batch_size = self.batch_size;
        let cancel_token = self.cancel_token.clone();
        tokio::spawn(async move {
            let listener = TcpListener::bind(listening_address).await.unwrap();
            loop {
                match listener.accept().await {
                    Ok((tcp_stream, socket_addr)) => {
                        info!("New connection from {socket_addr}");
                        tcp_stream.set_nodelay(true).unwrap();
                        tokio::spawn(Self::handle_incoming_connection(
                            tcp_stream,
                            None,
                            Some(client_sender.clone()),
                            Some(cluster_sender.clone()),
                            connection_sender.clone(),
                            Some(max_client_id_handle.clone()),
                            batch_size,
                            cancel_token.clone(),
                        ));
                    }
                    Err(e) => error!("Error listening for new connection: {:?}", e),
                }
            }
        })
    }

    async fn handle_incoming_connection(
        connection: TcpStream,
        cli_client_message_sender: Option<Sender<(RequestIdentifier, ClientMessage)>>,
        client_message_sender: Option<Sender<(ClientId, ClientMessage)>>,
        cluster_message_sender: Option<Sender<(NodeId, ClusterMessage)>>,
        connection_sender: Sender<NewConnection>,
        max_client_id_handle_opt: Option<Arc<Mutex<ClientId>>>,
        batch_size: usize,
        cancel_token: CancellationToken,
    ) {
        // Identify connector's ID and type by handshake
        let mut registration_connection = frame_registration_connection(connection);
        let registration_message = registration_connection.next().await;
        let new_connection = match registration_message {
            Some(Ok(RegistrationMessage::NodeRegister(node_id))) => {
                info!("Identified connection from node {node_id}");
                let underlying_stream = registration_connection.into_inner().into_inner();
                match cluster_message_sender {
                    Some(sender) => {
                        Some(NewConnection::ToPeer(PeerConnection::new(
                            node_id,
                            underlying_stream,
                            batch_size,
                            sender,
                            cancel_token,
                        )))
                    }
                    None => None
                }
            }
            Some(Ok(RegistrationMessage::ClientRegister)) => {
                match max_client_id_handle_opt {
                    Some(max_client_id_handle) => {
                        let next_client_id = {
                            let mut max_client_id = max_client_id_handle.lock().await;
                            *max_client_id += 1;
                            *max_client_id
                        };
                        info!("Identified connection from client {next_client_id}");
                        let underlying_stream = registration_connection.into_inner().into_inner();
                        match client_message_sender {
                            Some(sender) => {
                                Some(NewConnection::ToClient(ClientConnection::new(
                                    next_client_id,
                                    underlying_stream,
                                    batch_size,
                                    sender,
                                )))
                            }
                            None => None
                        }
                    }
                    None => { None }
                }
            }
            // Here we identify that it is a CLI connection
            Some(Ok(RegistrationMessage::CliClientRegister(request_identifier))) => {
                info!("Identified connection from cli client {:?}", request_identifier);
                let underlying_stream = registration_connection.into_inner().into_inner();
                match cli_client_message_sender {
                    Some(sender) => {
                        Some(NewConnection::ToCliClient(ClientConnection::new(
                            request_identifier,
                            underlying_stream,
                            batch_size,
                            sender,
                        )))
                    }
                    None => None
                }
            }
            Some(Err(err)) => {
                error!("Error deserializing handshake: {:?}", err);
                return;
            }
            None => {
                info!("Connection to unidentified source dropped");
                return;
            }
        };
        match new_connection {
            Some(connection) => {
                connection_sender.send(connection).await.unwrap();
            }
            None => {}
        }
    }

    fn spawn_peer_connectors(&self, connection_sender: Sender<NewConnection>) {
        let my_id = self.id;
        let peers_to_contact: Vec<NodeId> =
            self.peers.iter().cloned().filter(|&p| p > my_id).collect();
        for peer in peers_to_contact {
            let to_address = match get_node_addr(&self.cluster_name, peer, self.is_local) {
                Ok(addr) => addr,
                Err(e) => {
                    error!("Error resolving DNS name of node {peer}: {e}");
                    return;
                }
            };
            let reconnect_delay = Duration::from_secs(1);
            let mut reconnect_interval = tokio::time::interval(reconnect_delay);
            let cluster_sender = self.cluster_message_sender.clone();
            let connection_sender = connection_sender.clone();
            let batch_size = self.batch_size;
            let cancel_token = self.cancel_token.clone();
            tokio::spawn(async move {
                // Establish connection
                let peer_connection = loop {
                    reconnect_interval.tick().await;
                    match TcpStream::connect(to_address).await {
                        Ok(connection) => {
                            info!("New connection to node {peer}");
                            connection.set_nodelay(true).unwrap();
                            break connection;
                        }
                        Err(err) => {
                            error!("Establishing connection to node {peer} failed: {err}")
                        }
                    }
                };
                // Send handshake
                let mut registration_connection = frame_registration_connection(peer_connection);
                let handshake = RegistrationMessage::NodeRegister(my_id);
                if let Err(err) = registration_connection.send(handshake).await {
                    error!("Error sending handshake to {peer}: {err}");
                    return;
                }
                let underlying_stream = registration_connection.into_inner().into_inner();
                // Create connection actor
                let peer_actor = PeerConnection::new(
                    peer,
                    underlying_stream,
                    batch_size,
                    cluster_sender,
                    cancel_token,
                );
                let new_connection = NewConnection::ToPeer(peer_actor);
                connection_sender.send(new_connection).await.unwrap();
            });
        }
    }

    pub async fn send_to_cluster(&self, to: NodeId, msg: ClusterMessage) {
        let peer_conns_clone = Arc::clone(&self.peer_connections);
        let mut peer_connections = peer_conns_clone.lock().await;
        match self.cluster_id_to_idx(to) {
            Some(idx) => match peer_connections[idx] {
                Some(ref mut connection) => {
                    if let Err(err) = connection.send(msg) {
                        warn!("Couldn't send msg to peer {to}: {err}");
                        peer_connections[idx] = None;
                    }
                }
                None => warn!("Not connected to node {to}"),
            },
            None => error!("Sending to unexpected node {to}"),
        }
    }

    pub async fn send_to_client(&self, to: ClientId, msg: ServerMessage) {
        let conns = Arc::clone(&self.client_connections);
        let mut connections = conns.lock().await;
        match connections.get_mut(&to) {
            Some(connection) => {
                if let Err(err) = connection.send(msg) {
                    warn!("Couldn't send msg to client {to}: {err}");
                    connections.remove(&to);
                }
            }
            None => warn!("Not connected to client {to}"),
        }
    }

    // Removes all peer connections, but waits for queued writes to the peers to finish first
    #[allow(dead_code)]
    pub async fn shutdown(&mut self) {
        self.cancel_token.cancel();
        let peer_conns_clone = Arc::clone(&self.peer_connections);
        let mut peer_connections = peer_conns_clone.lock().await;
        for peer_connection in peer_connections.drain(..) {
            if let Some(connection) = peer_connection {
                connection.wait_for_writes_and_shutdown().await;
            }
        }
        for _ in 0..self.peers.len() {
            peer_connections.push(None);
        }
    }

    #[inline]
    fn cluster_id_to_idx(&self, id: NodeId) -> Option<usize> {
        self.peers.iter().position(|&p| p == id)
    }
}

enum NewConnection {
    ToPeer(PeerConnection),
    ToClient(ClientConnection<ClientId>),
    ToCliClient(ClientConnection<RequestIdentifier>)
}

struct PeerConnection {
    peer_id: NodeId,
    writer_task: JoinHandle<()>,
    outgoing_messages: UnboundedSender<ClusterMessage>,
}

impl PeerConnection {
    pub fn new(
        peer_id: NodeId,
        connection: TcpStream,
        batch_size: usize,
        incoming_messages: Sender<(NodeId, ClusterMessage)>,
        cancel_token: CancellationToken,
    ) -> Self {
        let (reader, mut writer) = frame_cluster_connection(connection);
        // Reader Actor
        let _reader_task = tokio::spawn(async move {
            let mut buf_reader = reader.ready_chunks(batch_size);
            while let Some(messages) = buf_reader.next().await {
                for msg in messages {
                    match msg {
                        Ok(m) => {
                            if let Err(_) = incoming_messages.send((peer_id, m)).await {
                                break;
                            };
                        }
                        Err(err) => {
                            error!("Error deserializing message: {:?}", err);
                        }
                    }
                }
            }
        });
        // Writer Actor
        let (message_tx, mut message_rx) = mpsc::unbounded_channel();
        let writer_task = tokio::spawn(async move {
            let mut buffer = Vec::with_capacity(batch_size);
            loop {
                tokio::select! {
                    biased;
                    num_messages = message_rx.recv_many(&mut buffer, batch_size) => {
                        if num_messages == 0 { break; }
                        for msg in buffer.drain(..) {
                            if let Err(err) = writer.feed(msg).await {
                                error!("Couldn't send message to node {peer_id}: {err}");
                                break;
                            }
                        }
                        if let Err(err) = writer.flush().await {
                            error!("Couldn't send message to node {peer_id}: {err}");
                            break;
                        }
                    },
                    _ = cancel_token.cancelled() => {
                        // Try to empty outgoing message queue before exiting
                        while let Ok(msg) = message_rx.try_recv() {
                            if let Err(err) = writer.feed(msg).await {
                                error!("Couldn't send message to node {peer_id}: {err}");
                                break;
                            }
                        }
                        if let Err(err) = writer.flush().await {
                            error!("Couldn't send message to node {peer_id}: {err}");
                            break;
                        }

                        // Gracefully shut down the write half of the connection
                        let mut underlying_socket = writer.into_inner().into_inner();
                        if let Err(err) = underlying_socket.shutdown().await {
                            error!("Error shutting down the stream to node {peer_id}: {err}");
                        }
                        break;
                    }
                }
            }
            info!("Connection to node {peer_id} closed");
        });
        PeerConnection {
            peer_id,
            writer_task,
            outgoing_messages: message_tx,
        }
    }

    pub fn send(
        &mut self,
        msg: ClusterMessage,
    ) -> Result<(), mpsc::error::SendError<ClusterMessage>> {
        self.outgoing_messages.send(msg)
    }

    #[allow(dead_code)]
    async fn wait_for_writes_and_shutdown(self) {
        let _ = tokio::time::timeout(Duration::from_secs(5), self.writer_task).await;
    }
}

struct ClientConnection<T> where T: Send + Display + Clone + Debug + 'static {
    client_id: T,
    outgoing_messages: UnboundedSender<ServerMessage>,
}

impl <T> ClientConnection<T> where T: Send + Display + Clone + Debug + 'static {
    pub fn new(
        client_id: T,
        connection: TcpStream,
        batch_size: usize,
        incoming_messages: Sender<(T, ClientMessage)>,
    ) -> Self {
        let (reader, mut writer) = frame_servers_connection(connection);
        let client_id_clone = client_id.clone();
        // Reader Actor
        let _reader_task = tokio::spawn(async move {
            let mut buf_reader = reader.ready_chunks(batch_size);
            while let Some(messages) = buf_reader.next().await {
                for msg in messages {
                    let client_id_clone_l = client_id.clone();
                    match msg {
                        Ok(m) => incoming_messages.send((client_id_clone_l, m)).await.unwrap(),
                        Err(err) => error!("Error deserializing message: {:?}", err),
                    }
                }
            }
        });
        // Writer Actor
        let (message_tx, mut message_rx) = mpsc::unbounded_channel();
        let _writer_task = tokio::spawn(async move {
            let mut buffer = Vec::with_capacity(batch_size);
            while message_rx.recv_many(&mut buffer, batch_size).await != 0 {
                for msg in buffer.drain(..) {
                    if let Err(err) = writer.feed(msg).await {
                        error!("Couldn't send message to client: {err}");
                        return;
                    }
                }
                if let Err(err) = writer.flush().await {
                    error!("Couldn't send message to client: {err}");
                    return;
                }
            }
        });
        ClientConnection {
            client_id: client_id_clone,
            outgoing_messages: message_tx,
        }
    }

    pub fn send(
        &mut self,
        msg: ServerMessage,
    ) -> Result<(), mpsc::error::SendError<ServerMessage>> {
        self.outgoing_messages.send(msg)
    }
}