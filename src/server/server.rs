use crate::{configs::OmniPaxosServerConfig, database::Database, network::Network};
use chrono::Utc;
use log::*;
use omnipaxos::{
    ballot_leader_election::Ballot,
    messages::Message,
    storage::Storage,
    util::{LogEntry, NodeId},
    OmniPaxos, OmniPaxosConfig,
};
use omnipaxos_kv::common::{ds::*, messages::*, utils::Timestamp};
use omnipaxos_storage::memory_storage::MemoryStorage;
use std::{fs::File, io::Write, time::Duration};
use std::collections::HashMap;
use std::sync::Arc;
use crate::network::CliNetwork;

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;
const NETWORK_BATCH_SIZE: usize = 100;
const LEADER_WAIT: Duration = Duration::from_secs(1);
const ELECTION_TIMEOUT: Duration = Duration::from_secs(1);

struct ResponseValue {
    current_idx: usize,
    value: Option<String>
}

pub struct OmniPaxosServer {
    id: NodeId,
    database: Database,
    network: Network,
    cli_network: Arc<CliNetwork>,
    omnipaxos: OmniPaxosInstance,
    current_decided_idx: usize,
    omnipaxos_msg_buffer: Vec<Message<Command>>,
    output_file: File,
    config: OmniPaxosServerConfig,
    peers: Vec<NodeId>,
    read_requests: HashMap<RequestIdentifier, Vec<ResponseValue>>
}

impl OmniPaxosServer {
    pub async fn new(config: OmniPaxosServerConfig) -> Self {
        // Initialize OmniPaxos instance
        let mut storage: MemoryStorage<Command> = MemoryStorage::default();
        let init_leader_ballot = Ballot {
            config_id: 0,
            n: 1,
            priority: 0,
            pid: config.initial_leader,
        };
        storage
            .set_promise(init_leader_ballot)
            .expect("Failed to write to storage");
        let omnipaxos_config: OmniPaxosConfig = config.clone().into();
        let omnipaxos_msg_buffer = Vec::with_capacity(omnipaxos_config.server_config.buffer_size);
        let omnipaxos = omnipaxos_config.build(storage).unwrap();

        // Wait for client and server network connections to be established
        let network = Network::new(
            config.cluster_name.clone(),
            config.server_id,
            config.nodes.clone(),
            config.num_clients,
            config.local_deployment.unwrap_or(false),
            NETWORK_BATCH_SIZE,
        )
        .await;

        let cli_network = CliNetwork::new(config.server_id, NETWORK_BATCH_SIZE);
        let output_file = File::create(config.output_filepath.clone()).unwrap();

        let db_config = config.db_config.clone();
        let mut server = OmniPaxosServer {
            id: config.server_id,
            database: Database::new(db_config).await,
            network,
            cli_network: Arc::new(cli_network),
            omnipaxos,
            current_decided_idx: 0,
            omnipaxos_msg_buffer,
            output_file,
            peers: config.get_peers(config.server_id),
            config,
            read_requests: HashMap::new()
        };
        // Save config to output file
        server.save_output().expect("Failed to write to file");
        server
    }

    pub async fn run(&mut self) {
        let mut client_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut cli_client_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        // We don't use Omnipaxos leader election and instead force an initial leader
        // Once the leader is established it chooses a synchronization point which the
        // followers relay to their clients to begin the experiment.
        if self.config.initial_leader == self.id {
            self.become_initial_leader(&mut cluster_msg_buf, &mut client_msg_buf)
                .await;
            let experiment_sync_start = (Utc::now() + Duration::from_secs(2)).timestamp_millis();
            self.send_cluster_start_signals(experiment_sync_start);
            self.send_client_start_signals(experiment_sync_start);
        }
        let cloned_cli_network = Arc::clone(&self.cli_network);
        tokio::spawn({
            async move {
                cloned_cli_network.listen_to_connections().await;
            }
        });
        // Main event loop
        let mut election_interval = tokio::time::interval(ELECTION_TIMEOUT);
        loop {
            let cli_net_clone = Arc::clone(&self.cli_network);
            tokio::select! {
                _ = election_interval.tick() => {
                    self.omnipaxos.tick();
                    self.send_outgoing_msgs();
                },
                _ = self.network.cluster_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE) => {
                    self.handle_cluster_messages(&mut cluster_msg_buf).await;
                },
                _ = self.network.client_messages.recv_many(&mut client_msg_buf, NETWORK_BATCH_SIZE) => {
                    self.handle_client_messages(&mut client_msg_buf).await;
                },
                _ = async {
                    let mut client_messages = cli_net_clone.cli_client_messages.lock().await;
                    client_messages.recv_many(&mut cli_client_msg_buf, NETWORK_BATCH_SIZE).await
                } => {
                    self.handle_cli_client_messages(&mut cli_client_msg_buf).await;
                },
            }
        }
    }

    // Ensures cluster is connected and leader is promoted before returning.
    async fn become_initial_leader(
        &mut self,
        cluster_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>,
        client_msg_buffer: &mut Vec<(ClientId, ClientMessage)>,
    ) {
        let mut leader_takeover_interval = tokio::time::interval(LEADER_WAIT);
        loop {
            tokio::select! {
                _ = leader_takeover_interval.tick() => {
                    if let Some((curr_leader, is_accept_phase)) = self.omnipaxos.get_current_leader(){
                        if curr_leader == self.id && is_accept_phase {
                            info!("{}: Leader fully initialized", self.id);
                            break;
                        }
                    }
                    info!("{}: Attempting to take leadership", self.id);
                    self.omnipaxos.try_become_leader();
                    self.send_outgoing_msgs();
                },
                _ = self.network.cluster_messages.recv_many(cluster_msg_buffer, NETWORK_BATCH_SIZE) => {
                    self.handle_cluster_messages(cluster_msg_buffer).await;
                },
                _ = self.network.client_messages.recv_many(client_msg_buffer, NETWORK_BATCH_SIZE) => {
                    self.handle_client_messages(client_msg_buffer).await;
                },
            }
        }
    }

    async fn handle_decided_entries(&mut self) {
        // TODO: Can use a read_raw here to avoid allocation
        let new_decided_idx = self.omnipaxos.get_decided_idx();
        if self.current_decided_idx < new_decided_idx {
            let decided_entries = self
                .omnipaxos
                .read_decided_suffix(self.current_decided_idx)
                .unwrap();
            self.current_decided_idx = new_decided_idx;
            debug!("Decided {new_decided_idx}");
            let decided_commands = decided_entries
                .into_iter()
                .filter_map(|e| match e {
                    LogEntry::Decided(cmd) => Some(cmd),
                    _ => unreachable!(),
                })
                .collect();
            self.update_database_and_respond(decided_commands).await;
        }
    }

    async fn update_database_and_respond(&mut self, commands: Vec<Command>) {
        // TODO: batching responses possible here (batch at handle_cluster_messages)
        for command in commands {
            let read = self.database.handle_command(command.ds_cmd).await;
            if command.coordinator_id == self.id {
                let response = match read {
                    Some(read_result) => ServerMessage::Read(command.id, read_result),
                    None => ServerMessage::Write(command.id),
                };
                self.network.send_to_client(command.client_id, response);
            }
        }
    }

    fn send_outgoing_msgs(&mut self) {
        self.omnipaxos
            .take_outgoing_messages(&mut self.omnipaxos_msg_buffer);
        for msg in self.omnipaxos_msg_buffer.drain(..) {
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
            self.network.send_to_cluster(to, cluster_msg);
        }
    }

    async fn handle_client_messages(&mut self, messages: &mut Vec<(ClientId, ClientMessage)>) {
        println!("Receiving client message: {:?}", messages);
        for (from, message) in messages.drain(..) {
            match message {
                ClientMessage::Append(command_id, kv_command) => {
                    self.append_to_log(from, command_id, kv_command)
                }
                ClientMessage::Read(request_identifier, consistency_level, ds_command) => {
                    self.handle_datasource_command(request_identifier, consistency_level, ds_command).await
                }
            }
        }
        self.send_outgoing_msgs();
    }

    async fn handle_cli_client_messages(&mut self, messages: &mut Vec<(RequestIdentifier, ClientMessage)>) {
        for (_, message) in messages.drain(..) {
            match message {
                ClientMessage::Read(request_identifier, consistency_level, command) => {
                    self.handle_datasource_command(request_identifier, consistency_level, command).await
                }
                _ => {
                    error!("Cli Client Send a non-read command");
                }
            }
        }
        self.send_outgoing_msgs();
    }

    async fn handle_cluster_messages(&mut self, messages: &mut Vec<(NodeId, ClusterMessage)>) {
        for (_from, message) in messages.drain(..) {
            trace!("{}: Received {message:?}", self.id);
            match message {
                ClusterMessage::OmniPaxosMessage(m) => {
                    self.omnipaxos.handle_incoming(m);
                    self.handle_decided_entries().await;
                }
                ClusterMessage::LeaderStartSignal(start_time) => {
                    self.send_client_start_signals(start_time)
                }
                ClusterMessage::ReadRequest(request_identifier, consistency_level, command) => {
                    info!("{}: Node: {}, as requested, I am querying database of server: {}", request_identifier, self.id, self.id);
                    let res = self.database.handle_command(command).await;
                    match res {
                        Some(opt) => {
                            self.network.send_to_cluster(_from, ClusterMessage::ReadResponse(request_identifier, consistency_level, self.current_decided_idx, opt))
                        }
                        None => {
                            self.network.send_to_cluster(_from, ClusterMessage::ReadResponse(request_identifier, consistency_level, self.current_decided_idx, None))
                        }
                    }
                }
                ClusterMessage::ReadResponse(request_identifier, consistency_level, current_decided_idx, response_option) => {
                    match consistency_level {
                        ConsistencyLevel::Leader => {
                            self.cli_network.send_to_cli_client(request_identifier.clone(), ServerMessage::ReadResponse(request_identifier, consistency_level, response_option)).await;
                        }
                        ConsistencyLevel::Linearizable => {
                            if let Some(requests) = self.read_requests.get_mut(&request_identifier) {
                                requests.push(ResponseValue { current_idx: current_decided_idx, value: response_option });
                                let number_of_responses = requests.len();
                                let majority = (self.peers.len() + 1) / 2;
                                if number_of_responses > majority {
                                    info!("{}: Node: {}, After receiving {} responses for number of nodes: {}, we can respond with the linearizable answer",
                                        request_identifier, self.id, number_of_responses, self.peers.len() + 1);
                                    let linearizable_response = self.get_linearizable_response(request_identifier.clone());
                                    self.read_requests.remove(&request_identifier);
                                    self.cli_network.send_to_cli_client(request_identifier.clone(),
                                        ServerMessage::ReadResponse(request_identifier, consistency_level, linearizable_response),
                                    ).await;
                                }
                            }
                        }
                        ConsistencyLevel::Local => {
                            // TODO can't happen
                        }
                    }
                }
            }
        }
        self.send_outgoing_msgs();
    }

    fn append_to_log(&mut self, from: ClientId, command_id: CommandId, ds_command: DataSourceCommand) {
        let command = Command {
            client_id: from,
            coordinator_id: self.id,
            id: command_id,
            ds_cmd: ds_command,
        };
        self.omnipaxos
            .append(command)
            .expect("Append to Omnipaxos log failed");
    }

    fn send_cluster_start_signals(&mut self, start_time: Timestamp) {
        for peer in &self.peers {
            debug!("Sending start message to peer {peer}");
            let msg = ClusterMessage::LeaderStartSignal(start_time);
            self.network.send_to_cluster(*peer, msg);
        }
    }

    fn send_client_start_signals(&mut self, start_time: Timestamp) {
        for client_id in 1..self.config.num_clients as ClientId + 1 {
            debug!("Sending start message to client {client_id}");
            let msg = ServerMessage::StartSignal(start_time);
            self.network.send_to_client(client_id, msg);
        }
    }

    fn save_output(&mut self) -> Result<(), std::io::Error> {
        let config_json = serde_json::to_string_pretty(&self.config)?;
        self.output_file.write_all(config_json.as_bytes())?;
        self.output_file.flush()?;
        Ok(())
    }

    async fn handle_datasource_command(&mut self, request_identifier: RequestIdentifier, consistency_level: ConsistencyLevel, command: DataSourceCommand) {
        match consistency_level {
            ConsistencyLevel::Local => {
                info!("{}: Node: {}, Received local command, querying database of server: {}", request_identifier, self.id, self.id);
                self.handle_local_datasource_command(request_identifier, consistency_level, command).await
            }
            ConsistencyLevel::Leader => {
                let leader_option = self.omnipaxos.get_current_leader();
                match leader_option {
                    Some((leader_id, _)) => {
                        if self.id == leader_id {
                            info!("{}: Node: {}, Received leader command, I am the leader, querying database of server: {}", request_identifier, self.id, self.id);
                            self.handle_local_datasource_command(request_identifier, consistency_level, command).await
                        } else {
                            info!("{}: Node: {}, Received leader command, leader is server: {}", request_identifier, self.id, leader_id);
                            self.network.send_to_cluster(leader_id, ClusterMessage::ReadRequest(request_identifier, consistency_level, command))
                        }
                    }
                    None => {
                        warn!("{}: Node: {}, Received leader command, no leader is available", request_identifier, self.id);
                        //TODO Check what should happen here
                    }
                }
            }
            ConsistencyLevel::Linearizable => {
                info!("{}: Node: {}, Received linearizable command, sending read request to all peers", request_identifier, self.id);
                let res = self.get_local_result(command.clone()).await;
                self.read_requests.insert(request_identifier.clone(), vec![ResponseValue { current_idx: self.current_decided_idx, value: res }]);
                for peer in &self.peers {
                    self.network.send_to_cluster(*peer, ClusterMessage::ReadRequest(request_identifier.clone(), consistency_level.clone(), command.clone()))
                }
            }
        }
    }

    async fn get_local_result(&mut self, data_source_command: DataSourceCommand) -> Option<String> {
        let res = self.database.handle_command(data_source_command).await;
        match res {
            Some(r) => { r }
            None => { None }
        }
    }
    async fn handle_local_datasource_command(&mut self, request_identifier: RequestIdentifier, consistency_level: ConsistencyLevel, command: DataSourceCommand) {
        let res = self.get_local_result(command).await;
        self.cli_network.send_to_cli_client(request_identifier.clone(), ServerMessage::ReadResponse(request_identifier, consistency_level, res)).await;
    }

    fn get_linearizable_response(&self, request_identifier: RequestIdentifier) -> Option<String> {
        if let Some(response) = self.read_requests.get(&request_identifier.clone())
            .and_then(|responses| responses.iter().max_by_key(|r| r.current_idx)) {
            response.value.clone()
        } else {
            None
        }
    }
}
