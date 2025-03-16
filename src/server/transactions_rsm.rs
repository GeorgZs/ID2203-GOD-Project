use std::sync::{Arc};
use futures::future::BoxFuture;
use tokio::sync::Mutex;
use omnipaxos_kv::common::ds::{Command, CommandType, NodeId};
use omnipaxos_kv::common::messages::ServerMessage;
use crate::database::Database;
use crate::network::Network;
use crate::omnipaxos_rsm::RSMConsumer;

pub struct TransactionsRSMConsumer {
    id: NodeId,
    network: Arc<Network>,
    database: Arc<Mutex<Database>>,
}

impl RSMConsumer for TransactionsRSMConsumer {
    fn new(id: NodeId, network: Arc<Network>, database: Arc<Mutex<Database>>) -> TransactionsRSMConsumer {
        TransactionsRSMConsumer { id, network, database }
    }

    fn get_network(&self) -> Arc<Network> {
        Arc::clone(&self.network)
    }

    fn handle_decided_entries(&mut self, commands: Vec<Command>) -> BoxFuture<()> {
        Box::pin(async move {
            for command in commands {
                let lock = Arc::clone(&self.database);
                let mut db = lock.lock().await;
                let mut result_found: bool = false;
                let mut buffer = String::new();
                match command.cmd_type {
                    CommandType::DatasourceCommand => {
                        let read = db.handle_command(command.ds_cmd.unwrap()).await;
                        if let Some(Some(rd)) = read {
                            buffer.push_str(rd.as_str());
                            result_found = true;
                        }
                    }
                    CommandType::TransactionCommand => {
                        for ds_cmd in command.tx_cmd.unwrap().data_source_commands {
                            let read = db.handle_command(ds_cmd).await;
                            if let Some(Some(rd)) = read {
                                buffer.push_str(rd.as_str());
                                result_found = true;
                            }
                        }
                    }
                }
                if command.coordinator_id == self.id {
                    let response = match result_found {
                        true => ServerMessage::Read(command.id, Some(buffer.clone())),
                        false => ServerMessage::Write(command.id),
                    };
                    self.network.send_to_client(command.client_id, response).await;
                }
            }
        })
    }
}