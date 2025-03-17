use std::collections::HashMap;
use std::sync::Arc;
use futures::future::BoxFuture;
use tokio::sync::Mutex;
use omnipaxos_kv::common::ds::{Command, CommandType, NodeId};
use omnipaxos_kv::common::messages::{ClusterMessage, TableName};
use crate::database::Database;
use crate::omnipaxos_rsm::RSMConsumer;

pub struct ShardRSMConsumer {
    database: Arc<Mutex<Database>>
}

impl ShardRSMConsumer {
    pub fn new(database: Arc<Mutex<Database>>) -> Self {
        ShardRSMConsumer { database }
    }
}

impl RSMConsumer for ShardRSMConsumer {
    fn send_to_cluster(&self,_: u64,  _: ClusterMessage) -> BoxFuture<()> {
        Box::pin(async move {
            // Do nothing
        })
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