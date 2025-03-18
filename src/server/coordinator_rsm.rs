use std::collections::HashMap;
use std::sync::Arc;
use futures::future::BoxFuture;
use log::info;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use omnipaxos_kv::common::ds::{Command, NodeId, TransactionId, TwoPhaseCommitState};
use omnipaxos_kv::common::ds::CommandType::TransactionCommand;
use omnipaxos_kv::common::messages::{ClusterMessage};
use crate::coordinator_rsm::TwoPhaseCommitAckType::Begin;
use crate::network::Network;
use crate::omnipaxos_rsm::{OmniPaxosRSM, RSMConsumer};
use crate::database::Database;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
enum TwoPhaseCommitAckType {
    Begin,
    Written,
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
            let tx_id_opt = cmd.tx_id.clone();
            if let Some(tx_id) = tx_id_opt {
                let ack_resp_cl = Arc::clone(&self.ack_responses);
                let mut ack_responses = ack_resp_cl.lock().await;
                if let Some(tx_acks) = ack_responses.get_mut(&tx_id) {
                    if let None = tx_acks.get(&two_phase_commit_ack_type) {
                        tx_acks.insert(two_phase_commit_ack_type.clone(), 0);
                    }
                    if let Some(acks) = tx_acks.get_mut(&two_phase_commit_ack_type) {
                        *acks += 1;
                        return *acks == self.peers.len() + 1;
                    }
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
                            TransactionCommand => {
                                let cmd = command.clone();
                                let tx_id_opt = cmd.tx_id.clone();
                                if let Some(tx_id) = tx_id_opt {
                                    let ack_resp_cl = Arc::clone(&self.ack_responses);
                                    let mut ack_responses = ack_resp_cl.lock().await;
                                    if let None = ack_responses.get(&tx_id) {
                                        ack_responses.insert(tx_id.clone(), HashMap::new());
                                    }
                                    let db_cl = Arc::clone(&self.database);
                                    let db = db_cl.lock().await;
                                    let _ = db.begin_tx(tx_id.clone()).await;
                                    let tx_acks = ack_responses.get_mut(&tx_id).unwrap();
                                    let num_acks = tx_acks.get(&TwoPhaseCommitAckType::Begin).unwrap_or(&0);
                                    tx_acks.insert(TwoPhaseCommitAckType::Begin, num_acks + 1);
                                    for peer in &self.peers {
                                        let netw_cl = Arc::clone(&self.network);
                                        netw_cl.send_to_cluster(*peer, ClusterMessage::BeginTransaction(command.clone())).await;
                                    }
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
                    let comm = command.clone();
                    let acks_met = self.transaction_reply(comm.clone(), Begin).await;
                    if acks_met {
                        let rsm_cl = Arc::clone(&self.coordinator_rsm);
                        let mut rsm = rsm_cl.lock().await;
                        let cmd = Command {
                            client_id: command.client_id,
                            coordinator_id: command.coordinator_id,
                            id: command.id,
                            tx_id: comm.tx_id,
                            two_phase_commit_state: Some(TwoPhaseCommitState::Begin),
                            total_number_of_commands: None,
                            cmd_type: TransactionCommand,
                            ds_cmd: None,
                            tx_cmd: command.tx_cmd,
                        };
                        rsm.append_to_log(cmd);
                    }
                }
                ClusterMessage::PrepareTransactionReply(command) => {
                    let comm = command.clone();
                    let acks_met = self.transaction_reply(command.clone(), TwoPhaseCommitAckType::Prepare).await;
                    if acks_met {
                        let rsm_cl = Arc::clone(&self.coordinator_rsm);
                        let mut rsm = rsm_cl.lock().await;
                        let cmd = Command {
                            client_id: command.client_id,
                            coordinator_id: command.coordinator_id,
                            id: command.id,
                            tx_id: comm.tx_id,
                            two_phase_commit_state: Some(TwoPhaseCommitState::Commit),
                            total_number_of_commands: None,
                            cmd_type: TransactionCommand,
                            ds_cmd: None,
                            tx_cmd: command.tx_cmd,
                        };
                        rsm.append_to_log(cmd);
                    }
                }
                ClusterMessage::WrittenAllQueriesReply(command) => {
                    let comm = command.clone();
                    let acks_met = self.transaction_reply(command.clone(), TwoPhaseCommitAckType::Written).await;
                    if acks_met {
                        let rsm_cl = Arc::clone(&self.coordinator_rsm);
                        let mut rsm = rsm_cl.lock().await;
                        let cmd = Command {
                            client_id: command.client_id,
                            coordinator_id: command.coordinator_id,
                            id: command.id,
                            tx_id: comm.tx_id,
                            two_phase_commit_state: Some(TwoPhaseCommitState::Prepare),
                            total_number_of_commands: None,
                            cmd_type: command.cmd_type,
                            ds_cmd: command.ds_cmd,
                            tx_cmd: command.tx_cmd,
                        };
                        rsm.append_to_log(cmd);
                    }
                }
                ClusterMessage::TransactionError(command, prepared) => {
                    let rsm_cl = Arc::clone(&self.coordinator_rsm);
                    let mut rsm = rsm_cl.lock().await;
                    let cmd = Command {
                        client_id: command.client_id,
                        coordinator_id: command.coordinator_id,
                        id: command.id,
                        tx_id: command.tx_id,
                        two_phase_commit_state: Some(if prepared { TwoPhaseCommitState::RollbackPrepared } else { TwoPhaseCommitState::Rollback }),
                        total_number_of_commands: None,
                        cmd_type: TransactionCommand,
                        ds_cmd: command.ds_cmd,
                        tx_cmd: command.tx_cmd,
                    };
                    rsm.append_to_log(cmd);
                }
                _ => {

                }
            }
        })
    }
}
