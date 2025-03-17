use std::collections::HashMap;
use std::sync::{Arc};
use futures::future::BoxFuture;
use tokio::sync::Mutex;
use omnipaxos_kv::common::ds::{Command, CommandType, NodeId};
use omnipaxos_kv::common::messages::{ClusterMessage, ServerMessage, TableName};
use crate::database::Database;
use crate::network::Network;
use crate::omnipaxos_rsm::{OmniPaxosRSM, RSMConsumer};

pub struct TransactionsRSMConsumer {
    id: NodeId,
    network: Arc<Network>,
    shard_leader_config: HashMap<TableName, NodeId>,
    pub shard_leader_rsm: Option<Arc<Mutex<OmniPaxosRSM>>>
}

impl TransactionsRSMConsumer {
    pub fn new(id: NodeId, network: Arc<Network>, _: Arc<Mutex<Database>>, shard_leader_config: HashMap<TableName, NodeId>) -> TransactionsRSMConsumer {
        TransactionsRSMConsumer { id, network, shard_leader_config, shard_leader_rsm: None }
    }
}

impl RSMConsumer for TransactionsRSMConsumer {
    fn send_to_cluster(&self,to: u64,  message: ClusterMessage) -> BoxFuture<()> {
        Box::pin(async move {
            self.network.send_to_cluster(to, message).await
        })
    }

    fn handle_decided_entries(&mut self, commands: Vec<Command>) -> BoxFuture<()> {
        Box::pin(async move {
            for command in commands {
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
                        for ds_cmd in command.tx_cmd.unwrap().data_source_commands {
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
            }
        })
    }
}