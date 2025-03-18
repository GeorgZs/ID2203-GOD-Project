use std::collections::HashMap;
use std::sync::{Arc};
use futures::future::BoxFuture;
use tokio::sync::Mutex;
use omnipaxos_kv::common::ds::{Command, CommandType, NodeId, TwoPhaseCommitState};
use omnipaxos_kv::common::messages::{ClusterMessage, ServerMessage, TableName};
use crate::database::Database;
use crate::network::Network;
use crate::omnipaxos_rsm::{OmniPaxosRSM, RSMConsumer};

pub struct TransactionsRSMConsumer {
    id: NodeId,
    network: Arc<Network>,
    shard_leader_config: HashMap<TableName, NodeId>,
    database: Arc<Mutex<Database>>,
    pub shard_leader_rsm: Option<Arc<Mutex<OmniPaxosRSM>>>
}

impl TransactionsRSMConsumer {
    pub fn new(id: NodeId, network: Arc<Network>, database: Arc<Mutex<Database>>, shard_leader_config: HashMap<TableName, NodeId>) -> TransactionsRSMConsumer {
        TransactionsRSMConsumer { id, network, shard_leader_config, database, shard_leader_rsm: None }
    }

    fn handle_coordinator_begin_command(&self, command: Command) -> BoxFuture<()> {
        Box::pin(async move {
            let command_id = command.id.clone();
            let client_id = command.client_id.clone();
            let coordination_id = command.coordinator_id.clone();
            match command.cmd_type {
                CommandType::DatasourceCommand => {
                    if let Some(ref ds_cmd) = command.ds_cmd {
                        if let Some(ref ds_obj) = ds_cmd.data_source_object {
                            if let Some(leader_id) = self.shard_leader_config.get(&ds_obj.table_name) {
                                if self.id == *leader_id {
                                    match self.shard_leader_rsm {
                                        Some(ref leader_rsm) => {
                                            let rsm_clone = Arc::clone(leader_rsm);
                                            let mut rsm = rsm_clone.lock().await;
                                            rsm.append_to_log(command)
                                        }
                                        None => {}
                                    }
                                }
                            }
                        }
                    }
                }
                CommandType::TransactionCommand => {
                    let ds_cmds = command.tx_cmd.unwrap().data_source_commands;
                    let len = ds_cmds.len().clone();
                    for ds_cmd in ds_cmds {
                        if let Some(ref ds_obj) = ds_cmd.data_source_object {
                            if let Some(leader_id) = self.shard_leader_config.get(&ds_obj.table_name) {
                                if self.id == *leader_id {
                                    match self.shard_leader_rsm {
                                        Some(ref leader_rsm) => {
                                            let rsm_clone = Arc::clone(leader_rsm);
                                            let mut rsm = rsm_clone.lock().await;
                                            let replication_command = Command {
                                                client_id,
                                                coordinator_id: command.coordinator_id,
                                                id: command_id,
                                                total_number_of_commands: Some(len),
                                                two_phase_commit_state: None,
                                                cmd_type: CommandType::DatasourceCommand,
                                                ds_cmd: Some(ds_cmd),
                                                tx_cmd: None
                                            };
                                            rsm.append_to_log(replication_command);
                                        }
                                        None => {}
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if coordination_id == self.id {
                self.network.send_to_client(client_id, ServerMessage::Write(command_id)).await;
            }
        })
    }
}

impl RSMConsumer for TransactionsRSMConsumer {
    fn get_network(&self) -> Arc<Network> {
        Arc::clone(&self.network)
    }

    fn handle_decided_entries(&mut self, _: Option<omnipaxos::util::NodeId>, commands: Vec<Command>) -> BoxFuture<()> {
        Box::pin(async move {
            for command in commands {
                if let Some(coordinator_command_state) = command.clone().two_phase_commit_state {
                    match coordinator_command_state {
                        TwoPhaseCommitState::Begin => {
                            self.handle_coordinator_begin_command(command).await;
                        }
                        TwoPhaseCommitState::Prepare => {
                            let lock = Arc::clone(&self.database);
                            let db = lock.lock().await;
                            let res = db.prepare_tx(command.clone().tx_cmd.unwrap().tx_id).await;
                            match res {
                                Ok(_) => {
                                    self.network.send_to_cluster(1, ClusterMessage::PrepareTransactionReply(command)).await;
                                }
                                Err(_) => {
                                    self.network.send_to_cluster(1, ClusterMessage::TransactionError(command, true)).await;
                                }
                            }
                        }
                        TwoPhaseCommitState::Commit => {
                            let lock = Arc::clone(&self.database);
                            let db = lock.lock().await;
                            let _ = db.commit_tx(command.tx_cmd.unwrap().tx_id).await;
                        }
                        TwoPhaseCommitState::Rollback => {
                            let lock = Arc::clone(&self.database);
                            let db = lock.lock().await;
                            let _ = db.rollback_tx(command.tx_cmd.unwrap().tx_id).await;
                        }
                        TwoPhaseCommitState::RollbackPrepared => {
                            let lock = Arc::clone(&self.database);
                            let db = lock.lock().await;
                            let _ = db.rollback_prepared_tx(command.tx_cmd.unwrap().tx_id).await;
                        }
                    }
                }
            }
        })
    }

    fn handle_cluster_message(&self, _: ClusterMessage) -> BoxFuture<()> {
        //do nothing
        Box::pin(async move {})
    }
}