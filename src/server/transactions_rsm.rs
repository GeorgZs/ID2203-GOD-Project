use std::sync::{Arc};
use futures::future::BoxFuture;
use tokio::sync::Mutex;
use omnipaxos_kv::common::ds::{Command, NodeId};
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
                let read = db.handle_command(command.ds_cmd).await;
                if command.coordinator_id == self.id {
                    let response = match read {
                        Some(read_result) => ServerMessage::Read(command.id, read_result),
                        None => ServerMessage::Write(command.id),
                    };
                    self.network.send_to_client(command.client_id, response).await;
                }
            }
        })
    }
}