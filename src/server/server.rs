use crate::{configs::OmniPaxosServerConfig, database::Database, network::Network};
use chrono::Utc;
use log::*;
use omnipaxos::{
    util::{NodeId}
};
use god_db::common::{ds::*, messages::*, utils::Timestamp};
use std::{fs::File, io::Write, time::Duration};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::coordinator_rsm::CoordinatorRSMConsumer;
use crate::network::CliNetwork;
use crate::omnipaxos_rsm::{OmniPaxosRSM, RSMConsumer};
use crate::shard_specific_manager::ShardSpecificManager;
use crate::transaction_stage_manager::TransactionStageManager;

const NETWORK_BATCH_SIZE: usize = 100;
const LEADER_WAIT: Duration = Duration::from_secs(1);
const ELECTION_TIMEOUT: Duration = Duration::from_secs(1);
const FOOD: &str = "food";
const DRINK: &str = "drink";
const DECORATION: &str = "decoration";


struct ResponseValue {
    current_idx: usize,
    value: Option<String>
}

pub struct OmniPaxosServer {
    id: NodeId,
    database: Arc<Mutex<Database>>,
    network: Arc<Network>,
    cli_network: Arc<CliNetwork>,
    // Abstraction of OmniPaxos from server
    omni_paxos_instances: HashMap<RSMIdentifier, Arc<Mutex<OmniPaxosRSM>>>,
    output_file: File,
    config: OmniPaxosServerConfig,
    peers: Vec<NodeId>,
    // Stores responses from other nodes - only when reading linearizable answers
    read_requests: HashMap<RequestIdentifier, Vec<ResponseValue>>
}

impl OmniPaxosServer {
    pub async fn new(config: OmniPaxosServerConfig) -> Self {
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
        let server_id = config.server_id.clone();
        let shard_leader_config = config.shard_leader_config.clone();
        let mut server = OmniPaxosServer {
            id: server_id,
            database: Arc::new(Mutex::new(Database::new(db_config).await)),
            network: Arc::new(network),
            cli_network: Arc::new(cli_network),
            omni_paxos_instances: HashMap::new(),
            output_file,
            peers: config.get_peers(server_id),
            config: config,
            read_requests: HashMap::new()
        };

        let shard_specific_manager = Arc::new(Mutex::new(ShardSpecificManager::new(server.id, Arc::clone(&server.database), Arc::clone(&server.network))));
        let food_omnipaxos_rsm = OmniPaxosRSM::new(RSMIdentifier::ShardSpecific(FOOD.to_string()), server.config.clone(), Arc::clone(&shard_specific_manager) as Arc<Mutex<dyn RSMConsumer>>);
        let drink_omnipaxos_rsm = OmniPaxosRSM::new(RSMIdentifier::ShardSpecific(DRINK.to_string()), server.config.clone(), Arc::clone(&shard_specific_manager) as Arc<Mutex<dyn RSMConsumer>>);
        let decoration_omnipaxos_rsm = OmniPaxosRSM::new(RSMIdentifier::ShardSpecific(DECORATION.to_string()), server.config.clone(), Arc::clone(&shard_specific_manager) as Arc<Mutex<dyn RSMConsumer>>);

        // Add rsm to omnipaxos instances
        server.omni_paxos_instances.insert(RSMIdentifier::ShardSpecific(FOOD.to_string()), food_omnipaxos_rsm);
        server.omni_paxos_instances.insert(RSMIdentifier::ShardSpecific(DRINK.to_string()), drink_omnipaxos_rsm);
        server.omni_paxos_instances.insert(RSMIdentifier::ShardSpecific(DECORATION.to_string()), decoration_omnipaxos_rsm);

        //Find table name for specific server id based on shard_leader_config
        let (table_name, _) = shard_leader_config.iter().find(|(_, &v)| v == server.id).unwrap();
        //Find leader_shard_rsm_identifier via table name
        let (leader_shard_rsm_identifier, _) = server.omni_paxos_instances.iter().find(|(k, _)| matches!(k, RSMIdentifier::ShardSpecific(name) if name == table_name)).unwrap();

        let mut transaction_stage_manager = TransactionStageManager::new(server_id, Arc::clone(&server.network), Arc::clone(&server.database), shard_leader_config.clone());
        transaction_stage_manager.shard_leader_rsm = Some(Arc::clone(server.omni_paxos_instances.get(&leader_shard_rsm_identifier).unwrap()));
        let transaction_stage_omnipaxos_rsm = OmniPaxosRSM::new(RSMIdentifier::TransactionStage, server.config.clone(), Arc::new(Mutex::new(transaction_stage_manager)) as Arc<Mutex<dyn RSMConsumer>>);

        let coordinating_manager = CoordinatorRSMConsumer::new(server_id, Arc::clone(&transaction_stage_omnipaxos_rsm), Arc::clone(&server.network), Arc::clone(&server.database), server.peers.clone());
        let client_requests_omnipaxos_rsm = OmniPaxosRSM::new(RSMIdentifier::ClientRequests, server.config.clone(), Arc::new(Mutex::new(coordinating_manager)) as Arc<Mutex<dyn RSMConsumer>>);

        server.omni_paxos_instances.insert(RSMIdentifier::ClientRequests, client_requests_omnipaxos_rsm);
        server.omni_paxos_instances.insert(RSMIdentifier::TransactionStage, transaction_stage_omnipaxos_rsm);
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
            self.send_cluster_start_signals(experiment_sync_start).await;
            self.send_client_start_signals(experiment_sync_start).await;
        }
        // Create a thread for keeping specifically CLI connections
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
            let network_clone = Arc::clone(&self.network);
            tokio::select! {
                // Every timeout check if leader is up or down or elect a new one
                _ = election_interval.tick() => {
                    for rsm in self.omni_paxos_instances.values_mut() {
                        let rsm_clone = Arc::clone(rsm);
                        let mut rsm_mut = rsm_clone.lock().await;
                        rsm_mut.handle_election_interval().await;
                    }
                },
                _ = async {
                    let mut cluster_messages = network_clone.cluster_messages.lock().await;
                    cluster_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE).await
                } => {
                    self.handle_cluster_messages(&mut cluster_msg_buf).await;
                },
                _ = async {
                    let mut client_messages = network_clone.client_messages.lock().await;
                    client_messages.recv_many(&mut client_msg_buf, NETWORK_BATCH_SIZE).await
                } => {
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
        let mut leader_instances: HashMap<RSMIdentifier, bool> = HashMap::new();
        for (rsm_identifier, _) in self.omni_paxos_instances.iter() {
            leader_instances.insert(rsm_identifier.clone(), false);
        }
        loop {
            let network_clone = Arc::clone(&self.network);
            tokio::select! {
                _ = leader_takeover_interval.tick() => {
                    for (rsm_identifier, rsm) in self.omni_paxos_instances.iter_mut() {
                        let rsm_clone = Arc::clone(rsm);
                        let mut rsm_mut = rsm_clone.lock().await;
                        let took_over_leadership = rsm_mut.handle_leader_takeover_interval().await;
                        if took_over_leadership {
                            leader_instances.insert(rsm_identifier.clone(), true);
                        }
                    }
                    if leader_instances.values().all(|&v| v) {
                        break;
                    }
                },
                _ = async {
                    let mut cluster_messages = network_clone.cluster_messages.lock().await;
                    cluster_messages.recv_many(cluster_msg_buffer, NETWORK_BATCH_SIZE).await
                } => {
                    self.handle_cluster_messages(cluster_msg_buffer).await;
                },
                _ = async {
                    let mut client_messages = network_clone.client_messages.lock().await;
                    client_messages.recv_many(client_msg_buffer, NETWORK_BATCH_SIZE).await
                } => {
                    self.handle_client_messages(client_msg_buffer).await;
                },
            }
        }
    }

    async fn handle_client_messages(&mut self, messages: &mut Vec<(ClientId, ClientMessage)>) {
        for (from, message) in messages.drain(..) {
            match message {
                ClientMessage::Append(command_id, tx_cmd) => {
                    self.append_to_log(from, command_id, tx_cmd).await;
                }
                ClientMessage::Read(request_identifier, consistency_level, ds_command) => {
                    self.handle_datasource_command(request_identifier, consistency_level, ds_command).await
                }
            }
        }
        for rsm in self.omni_paxos_instances.values_mut() {
            let rsm_clone = Arc::clone(rsm);
            let mut rsm_mut = rsm_clone.lock().await;
            rsm_mut.send_outgoing_msgs().await;
        }
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
        for rsm in self.omni_paxos_instances.values_mut() {
            let rsm_clone = Arc::clone(rsm);
            let mut rsm_mut = rsm_clone.lock().await;
            rsm_mut.send_outgoing_msgs().await;
        }
    }

    async fn handle_cluster_messages(&mut self, messages: &mut Vec<(NodeId, ClusterMessage)>) {
        for (_from, message) in messages.drain(..) {
            match message.clone() {
                ClusterMessage::OmniPaxosMessage(rsm_identifier, m) => {
                    let omnipaxos_instance = self.omni_paxos_instances.get_mut(&rsm_identifier).unwrap();
                    let rsm_clone = Arc::clone(omnipaxos_instance);
                    let mut rsm_mut = rsm_clone.lock().await;
                    let coordinator_id = if rsm_identifier != RSMIdentifier::ClientRequests {
                        let coord_cl = self.omni_paxos_instances.get(&RSMIdentifier::ClientRequests).unwrap();
                        let coordinator = coord_cl.lock().await;
                        match coordinator.get_current_leader() {
                            Some((ld_id, _)) => Some(ld_id),
                            None => None
                        }
                    } else {
                        match rsm_mut.get_current_leader() {
                            Some((ld_id, _)) => Some(ld_id),
                            None => None
                        }
                    };
                    rsm_mut.handle_incoming(m);
                    rsm_mut.handle_decided_entries(coordinator_id).await;
                }
                ClusterMessage::LeaderStartSignal(start_time) => {
                    self.send_client_start_signals(start_time).await;
                }
                ClusterMessage::BeginTransaction(command) => {
                    let db_clone = Arc::clone(&self.database);
                    let db = db_clone.lock().await;
                    let cmd = command.clone();
                    let tx_id_opt = cmd.tx_id.clone();
                    if let Some(tx_id) = tx_id_opt {
                        let _ = db.begin_tx(tx_id.clone()).await;
                        self.network.send_to_cluster(1, ClusterMessage::BeginTransactionReply(command)).await;
                    }
                }
                ClusterMessage::ReadRequest(request_identifier, consistency_level, command) => {
                    info!("{}: Node: {}, as requested, I am querying database of server: {}", request_identifier, self.id, self.id);
                    let db_clone = Arc::clone(&self.database);
                    let mut db = db_clone.lock().await;
                    let res = db.handle_command(command.clone()).await;
                    match res {
                        Ok(option) => {
                            match option {
                                Some(opt) => {
                                    let shard_rsm = self.get_shard_rsm_for_datasource_command(command).await;
                                    let rsm_mut = shard_rsm.lock().await;
                                    self.network.send_to_cluster(_from, ClusterMessage::ReadResponse(request_identifier, consistency_level, rsm_mut.current_decided_idx, opt)).await;
                                }
                                None => {
                                    let shard_rsm = self.get_shard_rsm_for_datasource_command(command).await;
                                    let rsm_mut = shard_rsm.lock().await;
                                    self.network.send_to_cluster(_from, ClusterMessage::ReadResponse(request_identifier, consistency_level, rsm_mut.current_decided_idx, None)).await;
                                }
                            }
                        },
                        Err(_) => {
                            let shard_rsm = self.get_shard_rsm_for_datasource_command(command).await;
                            let rsm_mut = shard_rsm.lock().await;
                            self.network.send_to_cluster(_from, ClusterMessage::ReadResponse(request_identifier, consistency_level, rsm_mut.current_decided_idx, None)).await;
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
                _ => {
                    let coordinator_rsm = self.omni_paxos_instances.get(&RSMIdentifier::ClientRequests).unwrap();
                    let rsm_clone = Arc::clone(coordinator_rsm);
                    let rsm_mut = rsm_clone.lock().await;
                    rsm_mut.handle_cluster_message(message).await;
                }
            }
        }
        for rsm in self.omni_paxos_instances.values_mut() {
            let rsm_clone = Arc::clone(rsm);
            let mut rsm_mut = rsm_clone.lock().await;
            rsm_mut.send_outgoing_msgs().await;
        }
    }

    async fn append_to_log(&mut self, from: ClientId, command_id: CommandId, tx_cmd: TransactionCommand) {
        let command = Command {
            client_id: from,
            coordinator_id: self.id,
            id: command_id,
            tx_id: Some(tx_cmd.tx_id.clone()),
            total_number_of_commands: None,
            two_phase_commit_state: None,
            cmd_type: CommandType::TransactionCommand,
            ds_cmd: None,
            tx_cmd: Some(tx_cmd)
        };
        let rsm = self.omni_paxos_instances.get_mut(&RSMIdentifier::ClientRequests).unwrap();
        let rsm_clone = Arc::clone(rsm);
        let mut rsm_mut = rsm_clone.lock().await;
        rsm_mut.append_to_log(command);
    }

    async fn send_cluster_start_signals(&mut self, start_time: Timestamp) {
        for peer in &self.peers {
            debug!("Sending start message to peer {peer}");
            let msg = ClusterMessage::LeaderStartSignal(start_time);
            self.network.send_to_cluster(*peer, msg).await;
        }
    }

    async fn send_client_start_signals(&mut self, start_time: Timestamp) {
        for client_id in 1..self.config.num_clients as ClientId + 1 {
            debug!("Sending start message to client {client_id}");
            let msg = ServerMessage::StartSignal(start_time);
            self.network.send_to_client(client_id, msg).await;
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
                let leader_id = *self.config.shard_leader_config.get(&command.clone().query_params.unwrap().table_name).unwrap();
                if self.id == leader_id {
                    info!("{}: Node: {}, Received leader command, I am the leader, querying database of server: {}", request_identifier, self.id, self.id);
                    self.handle_local_datasource_command(request_identifier, consistency_level, command).await
                } else {
                    info!("{}: Node: {}, Received leader command, leader is server: {}", request_identifier, self.id, leader_id);
                    self.network.send_to_cluster(leader_id, ClusterMessage::ReadRequest(request_identifier, consistency_level, command)).await;
                }
            }
            ConsistencyLevel::Linearizable => {
                info!("{}: Node: {}, Received linearizable command, sending read request to all peers", request_identifier, self.id);
                let res = self.get_local_result(command.clone()).await;
                let shard_rsm = self.get_shard_rsm_for_datasource_command(command.clone()).await;
                let rsm_mut = shard_rsm.lock().await;
                self.read_requests.insert(request_identifier.clone(), vec![ResponseValue { current_idx: rsm_mut.current_decided_idx, value: res }]);
                for peer in &self.peers {
                    self.network.send_to_cluster(*peer, ClusterMessage::ReadRequest(request_identifier.clone(), consistency_level.clone(), command.clone())).await;
                }
            }
        }
    }

    async fn get_local_result(&mut self, data_source_command: DataSourceCommand) -> Option<String> {
        let db_clone = Arc::clone(&self.database);
        let mut db = db_clone.lock().await;
        let res = db.handle_command(data_source_command).await;
        match res {
            Ok(option) => {
                match option {
                    Some(r) => { r }
                    None => { None }
                }
            },
            Err(_) => { None }
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

    async fn get_shard_rsm_for_datasource_command(&self, data_source_command: DataSourceCommand) -> Arc<Mutex<OmniPaxosRSM>> {
        let table_name = data_source_command.query_params.unwrap().table_name.clone();
        let (leader_shard_rsm_identifier, _) = self.omni_paxos_instances.iter().find(|(k, _)| matches!(k, RSMIdentifier::ShardSpecific(name) if name == &table_name)).unwrap();
        Arc::clone(&self.omni_paxos_instances.get(leader_shard_rsm_identifier).unwrap())
    }
}
