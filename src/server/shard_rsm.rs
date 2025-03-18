use std::collections::HashMap;
use std::sync::Arc;
use futures::future::BoxFuture;
use omnipaxos::util::NodeId;
use tokio::sync::Mutex;
use omnipaxos_kv::common::ds::{Command, CommandType, TransactionId};
use omnipaxos_kv::common::messages::ClusterMessage;
use crate::database::Database;
use crate::network::Network;
use crate::omnipaxos_rsm::RSMConsumer;

pub struct ShardRSMConsumer {
    database: Arc<Mutex<Database>>,
    network: Arc<Network>,
    queries_written: Arc<Mutex<HashMap<TransactionId, u64>>>,
}

impl ShardRSMConsumer {
    pub fn new(database: Arc<Mutex<Database>>, network: Arc<Network>) -> Self {
        ShardRSMConsumer { database, network, queries_written: Arc::new(Mutex::new(HashMap::new())) }
    }
}

impl RSMConsumer for ShardRSMConsumer {
    fn get_network(&self) -> Arc<Network> {
        Arc::clone(&self.network)
    }

    fn handle_decided_entries(&mut self, _: Option<NodeId>, commands: Vec<Command>) -> BoxFuture<()> {
        Box::pin(async move {
            for command in commands {
                let lock = Arc::clone(&self.database);
                let mut db = lock.lock().await;
                match command.cmd_type {
                    CommandType::DatasourceCommand => {
                        let cmd = command.clone();
                        let ds_cmd = command.ds_cmd.unwrap();
                        let tx_id = ds_cmd.tx_id.clone();
                        let res = db.handle_command(ds_cmd.clone()).await;
                        if let Some(number_of_queries) = command.total_number_of_commands {
                            if res.is_err() {
                                self.network.send_to_cluster(1, ClusterMessage::TransactionError(cmd, false)).await;
                            } else {
                                let qw_cl = Arc::clone(&self.queries_written);
                                let mut queries_written = qw_cl.lock().await;
                                if let None = queries_written.get_mut(&tx_id.clone().unwrap()) {
                                    queries_written.insert(tx_id.clone().unwrap(), 0);
                                }
                                let tx_queries_written = queries_written.get_mut(&tx_id.unwrap()).unwrap();
                                *tx_queries_written += 1;
                                if *tx_queries_written as usize == number_of_queries {
                                    //todo coordinator id
                                    self.network.send_to_cluster(1, ClusterMessage::WrittenAllQueriesReply(cmd)).await;
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        })
    }

    fn handle_cluster_message(&self, _: ClusterMessage) -> BoxFuture<()> {
        //do nothing
        Box::pin(async move {})
    }
}