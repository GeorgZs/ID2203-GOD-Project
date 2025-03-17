use std::collections::HashMap;
use std::sync::Arc;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use omnipaxos_kv::common::ds::{Command, CommandType, NodeId, TransactionId, TwoPhaseCommitState};
use omnipaxos_kv::common::ds::CommandType::TransactionCommand;
use omnipaxos_kv::common::messages::{ClusterMessage};
use crate::coordinator_rsm::TwoPhaseCommitAckType::Begin;
use crate::network::Network;
use crate::omnipaxos_rsm::{OmniPaxosRSM, RSMConsumer};
use crate::database::Database;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
enum TwoPhaseCommitAckType {
    Begin,
    Prepare
}

pub struct CoordinatorRSMConsumer {
    id: NodeId,
    coordinator_rsm: Arc<Mutex<OmniPaxosRSM>>,
    network: Arc<Network>,
    database: Arc<Mutex<Database>>,
    peers: Vec<NodeId>,
    ack_responses: Arc<Mutex<HashMap<TransactionId, HashMap<TwoPhaseCommitAckType, usize>>>>
}

impl CoordinatorRSMConsumer {
    pub fn new(id: NodeId, coordinator_rsm: Arc<Mutex<OmniPaxosRSM>>, network: Arc<Network>, database: Arc<Mutex<Database>>, peers: Vec<NodeId>) -> CoordinatorRSMConsumer {
        CoordinatorRSMConsumer {id, coordinator_rsm, network, database, peers, ack_responses: Arc::new(Mutex::new(HashMap::new()))}
    }

    fn transaction_reply(&self, command: Command, two_phase_commit_ack_type: TwoPhaseCommitAckType) -> BoxFuture<bool> {
        Box::pin(async move {
            let cmd = command.clone();
            let tx_id = cmd.tx_cmd.unwrap().clone().tx_id.clone();
            let ack_resp_cl = Arc::clone(&self.ack_responses);
            let mut ack_responses = ack_resp_cl.lock().await;
            if let Some(tx_acks) = ack_responses.get_mut(&tx_id) {
                if let Some(acks) = tx_acks.get_mut(&two_phase_commit_ack_type) {
                    *acks += 1;
                    return *acks == self.peers.len() + 1;
                }
            }
            false
        })
    }

}

impl RSMConsumer for CoordinatorRSMConsumer {
    fn get_network(&self) -> Arc<Network> {
        Arc::clone(&self.network)
    }

    fn handle_decided_entries(&mut self, leader_id: Option<omnipaxos::util::NodeId>, commands: Vec<Command>) -> BoxFuture<()> {
        Box::pin(async move {
            if let Some(leader_id) = leader_id {
                if leader_id == self.id {
                    for command in commands {
                        match command.cmd_type {
                            CommandType::TransactionCommand => {
                                let cmd = command.clone();
                                let tx_id = cmd.tx_cmd.unwrap().clone().tx_id.clone();
                                let ack_resp_cl = Arc::clone(&self.ack_responses);
                                let mut ack_responses = ack_resp_cl.lock().await;
                                if let None = ack_responses.get(&tx_id) {
                                    ack_responses.insert(tx_id.clone(), HashMap::new());
                                }
                                let db_cl = Arc::clone(&self.database);
                                let db = db_cl.lock().await;
                                db.begin_tx(tx_id.clone()).await;
                                let tx_acks = ack_responses.get_mut(&tx_id).unwrap();
                                let num_acks = tx_acks.get(&TwoPhaseCommitAckType::Begin).unwrap_or(&0);
                                tx_acks.insert(TwoPhaseCommitAckType::Begin, num_acks + 1);
                                for peer in &self.peers {
                                    let netw_cl = Arc::clone(&self.network);
                                    netw_cl.send_to_cluster(*peer, ClusterMessage::BeginTransaction(command.clone())).await;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        })
    }

    fn handle_cluster_message(&self, message: ClusterMessage) -> BoxFuture<()> {
        Box::pin(async move {
            match message {
                ClusterMessage::BeginTransactionReply(command) => {
                    let acks_met = self.transaction_reply(command.clone(), Begin).await;
                    if acks_met {
                        let rsm_cl = Arc::clone(&self.coordinator_rsm);
                        let mut rsm = rsm_cl.lock().await;
                        let cmd = Command {
                            client_id: command.client_id,
                            coordinator_id: command.coordinator_id,
                            id: command.id,
                            two_phase_commit_state: Some(TwoPhaseCommitState::Begin),
                            cmd_type: CommandType::TransactionCommand,
                            ds_cmd: None,
                            tx_cmd: command.tx_cmd,
                        };
                        rsm.append_to_log(cmd);
                    }
                }
                ClusterMessage::PrepareTransactionReply(command) => {
                    let acks_met = self.transaction_reply(command.clone(), TwoPhaseCommitAckType::Prepare).await;
                    if acks_met {
                        let rsm_cl = Arc::clone(&self.coordinator_rsm);
                        let mut rsm = rsm_cl.lock().await;
                        let cmd = Command {
                            client_id: command.client_id,
                            coordinator_id: command.coordinator_id,
                            id: command.id,
                            two_phase_commit_state: Some(TwoPhaseCommitState::Commit),
                            cmd_type: TransactionCommand,
                            ds_cmd: None,
                            tx_cmd: command.tx_cmd,
                        };
                        rsm.append_to_log(cmd);
                    }
                }
                _ => {

                }
            }
        })
    }
}
