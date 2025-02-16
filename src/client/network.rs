use futures::{SinkExt, StreamExt};
use log::*;
use omnipaxos_kv::common::{kv::NodeId, messages::*, utils::*};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{self, channel};
use tokio::task::JoinHandle;
use tokio::{net::TcpStream, sync::mpsc::Receiver};
use tokio::{sync::mpsc::Sender, time::interval};
use tokio_util::sync::CancellationToken;

pub struct Network {
    cluster_name: String,
    is_local: bool,
    server_connections: Vec<Option<ServerConnection>>,
    batch_size: usize,
    server_message_sender: Sender<ServerMessage>,
    pub server_messages: Receiver<ServerMessage>,
    cancel_token: CancellationToken,
}

const RETRY_SERVER_CONNECTION_TIMEOUT: Duration = Duration::from_secs(1);

impl Network {
    pub async fn new(
        cluster_name: String,
        server_ids: Vec<NodeId>,
        local_deployment: bool,
        batch_size: usize,
    ) -> Self {
        let mut server_connections = vec![];
        let max_server_id = *server_ids.iter().max().unwrap() as usize;
        server_connections.resize_with(max_server_id + 1, Default::default);
        let (server_message_sender, server_messages) = channel(batch_size);
        let mut network = Self {
            cluster_name,
            is_local: local_deployment,
            server_connections,
            batch_size,
            server_message_sender,
            server_messages,
            cancel_token: CancellationToken::new(),
        };
        network.initialize_connections(server_ids).await;
        network
    }

    async fn initialize_connections(&mut self, server_ids: Vec<NodeId>) {
        info!("Establishing server connections");
        let mut connection_tasks = Vec::with_capacity(server_ids.len());
        for server_id in &server_ids {
            let server_address = get_node_addr(&self.cluster_name, *server_id, self.is_local)
                .expect("Couldn't resolve server IP");
            let task = tokio::spawn(Self::get_server_connection(*server_id, server_address));
            connection_tasks.push(task);
        }
        let finished_tasks = futures::future::join_all(connection_tasks).await;
        for (i, result) in finished_tasks.into_iter().enumerate() {
            match result {
                Ok((from_server_conn, to_server_conn)) => {
                    let connected_server_id = server_ids[i];
                    info!("Connected to server {connected_server_id}");
                    let server_idx = connected_server_id as usize;
                    let server_connection = ServerConnection::new(
                        connected_server_id,
                        from_server_conn,
                        to_server_conn,
                        self.batch_size,
                        self.server_message_sender.clone(),
                        self.cancel_token.clone(),
                    );
                    self.server_connections[server_idx] = Some(server_connection)
                }
                Err(err) => {
                    let failed_server = server_ids[i];
                    panic!("Unable to establish connection to server {failed_server}: {err}")
                }
            }
        }
    }

    async fn get_server_connection(
        server_id: NodeId,
        server_address: SocketAddr,
    ) -> (FromServerConnection, ToServerConnection) {
        let mut retry_connection = interval(RETRY_SERVER_CONNECTION_TIMEOUT);
        loop {
            retry_connection.tick().await;
            match TcpStream::connect(server_address).await {
                Ok(stream) => {
                    stream.set_nodelay(true).unwrap();
                    let mut registration_connection = frame_registration_connection(stream);
                    registration_connection
                        .send(RegistrationMessage::ClientRegister)
                        .await
                        .expect("Couldn't send registration to server");
                    let underlying_stream = registration_connection.into_inner().into_inner();
                    break frame_clients_connection(underlying_stream);
                }
                Err(e) => error!("Unable to connect to server {server_id}: {e}"),
            }
        }
    }

    pub async fn send(&mut self, to: NodeId, msg: ClientMessage) {
        match self.server_connections.get_mut(to as usize) {
            Some(connection_slot) => match connection_slot {
                Some(connection) => {
                    if let Err(err) = connection.send(msg).await {
                        warn!("Couldn't send msg to server {to}: {err}");
                        self.server_connections[to as usize] = None;
                    }
                }
                None => error!("Not connected to server {to}"),
            },
            None => error!("Sending to unexpected server {to}"),
        }
    }

    // Removes all server connections, but waits for queued writes to the servers to finish first
    pub async fn shutdown(&mut self) {
        self.cancel_token.cancel();
        let connection_count = self.server_connections.len();
        for server_connection in self.server_connections.drain(..) {
            if let Some(connection) = server_connection {
                connection.wait_for_writes().await;
            }
        }
        for _ in 0..connection_count {
            self.server_connections.push(None);
        }
    }
}

struct ServerConnection {
    // server_id: NodeId,
    writer_task: JoinHandle<()>,
    outgoing_messages: Sender<ClientMessage>,
}

impl ServerConnection {
    pub fn new(
        server_id: NodeId,
        reader: FromServerConnection,
        mut writer: ToServerConnection,
        batch_size: usize,
        incoming_messages: Sender<ServerMessage>,
        cancel_token: CancellationToken,
    ) -> Self {
        // Reader Actor
        let _reader_task = tokio::spawn(async move {
            let mut buf_reader = reader.ready_chunks(batch_size);
            while let Some(messages) = buf_reader.next().await {
                for msg in messages {
                    // debug!("Network: Response from server {server_id}: {msg:?}");
                    match msg {
                        Ok(m) => incoming_messages.send(m).await.unwrap(),
                        Err(err) => error!("Error deserializing message: {:?}", err),
                    }
                }
            }
        });
        // Writer Actor
        let (message_tx, mut message_rx) = mpsc::channel(batch_size);
        let writer_task = tokio::spawn(async move {
            let mut buffer = Vec::with_capacity(batch_size);
            loop {
                tokio::select! {
                    biased;
                    num_messages = message_rx.recv_many(&mut buffer, batch_size) => {
                        if num_messages == 0 { break; }
                        for msg in buffer.drain(..) {
                            if let Err(err) = writer.feed(msg).await {
                                error!("Couldn't send message to server {server_id}: {err}");
                                break;
                            }
                        }
                        if let Err(err) = writer.flush().await {
                            error!("Couldn't send message to server {server_id}: {err}");
                            break;
                        }
                    },
                    _ = cancel_token.cancelled() => {
                        // Try to empty outgoing message queue before exiting
                        while let Ok(msg) = message_rx.try_recv() {
                            if let Err(err) = writer.feed(msg).await {
                                error!("Couldn't send message to server {server_id}: {err}");
                                break;
                            }
                        }
                        if let Err(err) = writer.flush().await {
                            error!("Couldn't send message to server {server_id}: {err}");
                            break;
                        }

                        // Gracefully shut down the write half of the connection
                        let mut underlying_socket = writer.into_inner().into_inner();
                        if let Err(err) = underlying_socket.shutdown().await {
                            error!("Error shutting down the stream to server {server_id}: {err}");
                        }
                        break;
                    }
                }
            }
            info!("Connection to server {server_id} closed");
        });
        ServerConnection {
            // server_id,
            writer_task,
            outgoing_messages: message_tx,
        }
    }

    pub async fn send(
        &mut self,
        msg: ClientMessage,
    ) -> Result<(), mpsc::error::SendError<ClientMessage>> {
        self.outgoing_messages.send(msg).await
    }

    async fn wait_for_writes(self) {
        let _ = tokio::time::timeout(Duration::from_secs(5), self.writer_task).await;
    }
}
