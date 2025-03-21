use std::sync::Arc;
use futures::future::BoxFuture;
use log::{debug, info};
use omnipaxos::ballot_leader_election::Ballot;
use omnipaxos::messages::Message;
use omnipaxos::{OmniPaxos, OmniPaxosConfig};
use omnipaxos::storage::{Storage};
use omnipaxos::util::{LogEntry, NodeId};
use omnipaxos_storage::memory_storage::MemoryStorage;
use tokio::sync::Mutex;
use god_db::common::ds::{Command};
use god_db::common::messages::{ClusterMessage, RSMIdentifier};
use crate::configs::OmniPaxosServerConfig;
use crate::network::Network;

pub trait RSMConsumer: Send + Sync {
    fn get_network(&self) -> Arc<Network>;
    fn handle_decided_entries(&mut self, leader_id: Option<NodeId>, coordinator_id: Option<NodeId>, commands: Vec<Command>) -> BoxFuture<()>;
    fn handle_cluster_message(&self, message: ClusterMessage) -> BoxFuture<()>;
}

pub struct OmniPaxosRSM {
    rsm_identifier: RSMIdentifier,
    // Differentiates the functionality from every node, or which node should I listen from.
    id: NodeId,
    // Library object from omnipaxos
    omnipaxos: OmniPaxos<Command, MemoryStorage<Command>>,
    pub current_decided_idx: usize,
    // Buffer for msgs we have not sent yet
    omnipaxos_msg_buffer: Vec<Message<Command>>,
    // Allows composition inheritance
    consumer: Arc<Mutex<dyn RSMConsumer>>,
}

impl OmniPaxosRSM {
    pub fn new(rsm_identifier: RSMIdentifier, config: OmniPaxosServerConfig, consumer: Arc<Mutex<dyn RSMConsumer>>) -> Arc<Mutex<OmniPaxosRSM>> {
        let mut storage: MemoryStorage<Command> = MemoryStorage::default();
        let server_id = config.server_id.clone();
        let init_leader_ballot = Ballot {
            config_id: 0,
            n: 1,
            priority: 0,
            pid: config.initial_leader,
        };
        storage
            .set_promise(init_leader_ballot)
            .expect("Failed to write to storage");
        let omnipaxos_config: OmniPaxosConfig = config.into();
        let omnipaxos_msg_buffer = Vec::with_capacity(omnipaxos_config.server_config.buffer_size);
        let omnipaxos = omnipaxos_config.build(storage).unwrap();
        Arc::new(Mutex::new(OmniPaxosRSM {
            rsm_identifier,
            id: server_id,
            omnipaxos,
            current_decided_idx: 0,
            omnipaxos_msg_buffer,
            consumer
        }))
    }

    pub fn get_current_leader(&self) -> Option<(NodeId, bool)> {
        self.omnipaxos.get_current_leader()
    }

    pub fn append_to_log(&mut self, command: Command) {
        self.omnipaxos
            .append(command)
            .expect("Append to Omnipaxos log failed");
    }

    pub fn handle_incoming(&mut self, message: Message<Command>) {
        self.omnipaxos.handle_incoming(message);
    }

    pub async fn send_outgoing_msgs(&mut self) {
        self.omnipaxos
            .take_outgoing_messages(&mut self.omnipaxos_msg_buffer);
        for msg in self.omnipaxos_msg_buffer.drain(..) {
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(self.rsm_identifier.clone(), msg);
            let cons_cl = Arc::clone(&self.consumer);
            let consumer = cons_cl.lock().await;
            consumer.get_network().send_to_cluster(to, cluster_msg).await;
        }
    }

    pub async fn handle_election_interval (&mut self) {
        self.omnipaxos.tick();
        self.send_outgoing_msgs().await;
    }

    pub async fn handle_leader_takeover_interval (&mut self) -> bool {
        if let Some((curr_leader, is_accept_phase)) = self.omnipaxos.get_current_leader(){
            if curr_leader == self.id && is_accept_phase {
                info!("{}: Leader fully initialized", self.id);
                return true;
            }
        }
        info!("{}: Attempting to take leadership", self.id);
        self.omnipaxos.try_become_leader();
        self.send_outgoing_msgs().await;
        false
    }

    pub async fn handle_decided_entries(&mut self, coordinator_id: Option<NodeId>) {
        let leader_id = match self.omnipaxos.get_current_leader() {
            Some((ld_id, _)) => Some(ld_id),
            None => None
        };
        let new_decided_idx = self.omnipaxos.get_decided_idx();
        if self.current_decided_idx < new_decided_idx {
            let decided_entries = self
                .omnipaxos
                .read_decided_suffix(self.current_decided_idx)
                .unwrap();
            self.current_decided_idx = new_decided_idx;
            debug!("{:?}: Decided {new_decided_idx}", self.rsm_identifier);
            let decided_commands = decided_entries
                .into_iter()
                .filter_map(|e| match e {
                    LogEntry::Decided(cmd) => Some(cmd),
                    _ => unreachable!(),
                })
                .collect();
            let cons_cl = Arc::clone(&self.consumer);
            let mut consumer = cons_cl.lock().await;
            let future = consumer.handle_decided_entries(leader_id, coordinator_id, decided_commands);
            future.await;
        }
    }

    pub async fn handle_cluster_message(&self, message: ClusterMessage) {
        let cons_cl = Arc::clone(&self.consumer);
        let consumer = cons_cl.lock().await;
        consumer.handle_cluster_message(message).await;
    }
}