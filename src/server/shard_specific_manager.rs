use std::collections::HashMap;
use std::sync::Arc;
use futures::future::BoxFuture;
use omnipaxos::util::NodeId;
use tokio::sync::Mutex;
use god_db::common::ds::{Command, CommandType, TransactionId};
use god_db::common::messages::ClusterMessage;
use crate::database::Database;
use crate::network::Network;
use crate::omnipaxos_rsm::RSMConsumer;

pub struct ShardSpecificManager {
    id: NodeId,
    database: Arc<Mutex<Database>>,
    network: Arc<Network>,
    queries_written: Arc<Mutex<HashMap<TransactionId, u64>>>,
}

impl ShardSpecificManager {
    pub fn new(id: NodeId, database: Arc<Mutex<Database>>, network: Arc<Network>) -> Self {
        ShardSpecificManager { id, database, network, queries_written: Arc::new(Mutex::new(HashMap::new())) }
    }
}

impl RSMConsumer for ShardSpecificManager {
    fn get_network(&self) -> Arc<Network> {
        Arc::clone(&self.network)
    }

    fn handle_decided_entries(&mut self, _: Option<NodeId>, coordinator_id: Option<NodeId>, commands: Vec<Command>) -> BoxFuture<()> {
        Box::pin(async move {
            for command in commands {
                let lock = Arc::clone(&self.database);
                let mut db = lock.lock().await;
                match command.cmd_type {
                    CommandType::DatasourceCommand => {
                        let cmd = command.clone();
                        let ds_cmd = command.ds_cmd.unwrap();
                        let tx_id_opt = ds_cmd.tx_id.clone();
                        let res = db.handle_command(ds_cmd.clone()).await;
                        if let Some(number_of_queries) = command.total_number_of_commands {
                            if let Some(tx_id) = tx_id_opt {
                                if res.is_err() {
                                    self.network.send_to_cluster(1, ClusterMessage::TransactionError(cmd, false)).await;
                                } else {
                                    let qw_cl = Arc::clone(&self.queries_written);
                                    let mut queries_written = qw_cl.lock().await;
                                    if queries_written.get(&tx_id).is_none() {
                                        queries_written.insert(tx_id.clone(), 0);
                                    }
                                    if let Some(tx_queries_written) = queries_written.get_mut(&tx_id) {
                                        *tx_queries_written += 1;
                                        if *tx_queries_written as usize == number_of_queries {
                                            let coord_id = coordinator_id.unwrap();
                                            if self.id == coord_id {
                                                let _ = self.network.cluster_message_sender.send((coord_id, ClusterMessage::WrittenAllQueriesReply(cmd))).await;
                                            } else {
                                                self.network.send_to_cluster(coord_id, ClusterMessage::WrittenAllQueriesReply(cmd)).await;
                                            }
                                        }
                                    }
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