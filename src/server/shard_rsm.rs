use std::sync::Arc;
use futures::future::BoxFuture;
use tokio::sync::Mutex;
use omnipaxos_kv::common::ds::{Command, CommandType};
use crate::database::Database;
use crate::network::Network;
use crate::omnipaxos_rsm::RSMConsumer;

pub struct ShardRSMConsumer {
    database: Arc<Mutex<Database>>,
    network: Arc<Network>
}

impl ShardRSMConsumer {
    pub fn new(database: Arc<Mutex<Database>>, network: Arc<Network>) -> Self {
        ShardRSMConsumer { database, network }
    }
}

impl RSMConsumer for ShardRSMConsumer {
    fn get_network(&self) -> Arc<Network> {
        Arc::clone(&self.network)
    }

    fn handle_decided_entries(&mut self, commands: Vec<Command>) -> BoxFuture<()> {
        Box::pin(async move {
            for command in commands {
                let lock = Arc::clone(&self.database);
                let mut db = lock.lock().await;
                match command.cmd_type {
                    CommandType::DatasourceCommand => {
                        db.handle_command(command.ds_cmd.unwrap()).await;
                    }
                    CommandType::TransactionCommand => {
                        for ds_cmd in command.tx_cmd.unwrap().data_source_commands {
                            db.handle_command(ds_cmd).await;
                        }
                    }
                }
            }
        })
    }
}