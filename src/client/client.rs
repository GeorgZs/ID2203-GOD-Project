use crate::{data_collection::ClientData, network::Network};
use chrono::Utc;
use log::*;
use god_db::common::{ds::*, messages::*, utils::Timestamp};
use rand::{random, Rng};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use rand::distributions::{Alphanumeric, DistString};
use tokio::time::interval;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClientConfig {
    pub cluster_name: String,
    pub location: String,
    pub server_id: NodeId,
    pub requests: Vec<RequestInterval>,
    pub local_deployment: Option<bool>,
    pub sync_time: Option<Timestamp>,
    pub summary_filepath: String,
    pub output_filepath: String,
}

const NETWORK_BATCH_SIZE: usize = 100;

pub struct Client {
    id: ClientId,
    network: Network,
    client_data: ClientData,
    config: ClientConfig,
    active_server: NodeId,
    final_request_count: Option<usize>,
    next_request_id: usize,
}

impl Client {
    pub async fn new(config: ClientConfig) -> Self {
        let local_deployment = config.local_deployment.unwrap_or(false);
        let network = Network::new(
            config.cluster_name.clone(),
            vec![config.server_id],
            local_deployment,
            NETWORK_BATCH_SIZE,
        )
        .await;
        Client {
            id: config.server_id,
            network,
            client_data: ClientData::new(),
            active_server: config.server_id,
            config,
            final_request_count: None,
            next_request_id: 0,
        }
    }

    pub async fn run(&mut self) {
        // Wait for server to signal start
        info!("{}: Waiting for start signal from server", self.id);
        match self.network.server_messages.recv().await {
            Some(ServerMessage::StartSignal(start_time)) => {
                Self::wait_until_sync_time(&mut self.config, start_time).await;
            }
            _ => panic!("Error waiting for start signal"),
        }

        // Early end
        let intervals = self.config.requests.clone();
        if intervals.is_empty() {
            self.save_results().expect("Failed to save results");
            return;
        }

        // Initialize intervals
        let mut rng = rand::thread_rng();
        let mut intervals = intervals.iter();
        let first_interval = intervals.next().unwrap();
        let mut read_ratio = first_interval.read_ratio;
        let mut request_interval = interval(first_interval.get_request_delay());
        let mut next_interval = interval(first_interval.get_interval_duration());
        let _ = next_interval.tick().await;

        // Main event loop
        info!("{}: Starting requests", self.id);
        loop {
            tokio::select! {
                biased;
                Some(msg) = self.network.server_messages.recv() => {
                    self.handle_server_message(msg);
                    if self.run_finished() {
                        break;
                    }
                }
                _ = request_interval.tick(), if self.final_request_count.is_none() => {
                    let is_write = rng.gen::<f64>() > read_ratio;
                    self.send_request(is_write).await;
                },
                _ = next_interval.tick() => {
                    match intervals.next() {
                        Some(new_interval) => {
                            read_ratio = new_interval.read_ratio;
                            next_interval = interval(new_interval.get_interval_duration());
                            next_interval.tick().await;
                            request_interval = interval(new_interval.get_request_delay());
                        },
                        None => {
                            self.final_request_count = Some(self.client_data.request_count());
                            if self.run_finished() {
                                break;
                            }
                        },
                    }
                },
            }
        }

        info!(
            "{}: Client finished: collected {} responses",
            self.id,
            self.client_data.response_count(),
        );
        self.network.shutdown().await;
        self.save_results().expect("Failed to save results");
    }

    fn handle_server_message(&mut self, msg: ServerMessage) {
        match msg {
            ServerMessage::StartSignal(_) => (),
            server_response => {
                let cmd_id = server_response.command_id();
                self.client_data.new_response(cmd_id);
            }
        }
    }

    async fn send_request(&mut self, is_write: bool) {
        //TODO!!
        //randomly select ds_command as either insert or read
        let unique_identifier = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);

        let write_food_ds_command = DataSourceCommand {
            tx_id: Some(unique_identifier.clone()),
            query_type: DataSourceQueryType::INSERT,
            data_source_object: Some(DataSourceObject {
                table_name: String::from("food"),
                row_data: vec![
                    RowData{
                        row_name: String::from("name"),
                        row_value: String::from(random::<i32>().to_string())
                    },
                ]
            }),
            query_params: None,
        };

        let write_drink_ds_command = DataSourceCommand {
            tx_id: Some(unique_identifier.clone()),
            query_type: DataSourceQueryType::INSERT,
            data_source_object: Some(DataSourceObject {
                table_name: String::from("drink"),
                row_data: vec![
                    RowData{
                        row_name: String::from("name"),
                        row_value: String::from(random::<i32>().to_string())
                    },
                ]
            }),
            query_params: None,
        };

        let write_decoration_ds_command = DataSourceCommand {
            tx_id: Some(unique_identifier.clone()),
            query_type: DataSourceQueryType::INSERT,
            data_source_object: Some(DataSourceObject {
                table_name: String::from("decoration"),
                row_data: vec![
                    RowData{
                        row_name: String::from("name"),
                        row_value: String::from(random::<i32>().to_string())
                    },
                ]
            }),
            query_params: None,
        };

        /*let read_ds_command = DataSourceCommand {
            query_type: DataSourceQueryType::READ,
            data_source_object: None,
            query_params: Some(QueryParams {
                table_name: String::from("food"),
                select_all: true,
                select_columns: None,
            }),
        };*/

        let request;

        request = ClientMessage::Append(self.next_request_id, TransactionCommand {tx_id: unique_identifier, data_source_commands: vec![write_food_ds_command, write_drink_ds_command, write_decoration_ds_command]});

        /*match is_write {
            true => request = ClientMessage::Append(self.next_request_id, TransactionCommand {tx_id: unique_identifier, data_source_commands: vec![write_ds_command]}),
            false => request = ClientMessage::Append(self.next_request_id, TransactionCommand {tx_id: unique_identifier, data_source_commands: vec![read_ds_command]}),
        };*/

        self.network.send(self.active_server, request).await;
        self.client_data.new_request(is_write);
        self.next_request_id += 1;
    }

    fn run_finished(&self) -> bool {
        if let Some(count) = self.final_request_count {
            if self.client_data.request_count() >= count {
                return true;
            }
        }
        return false;
    }

    // Wait until the scheduled start time to synchronize client starts.
    // If start time has already passed, start immediately.
    async fn wait_until_sync_time(config: &mut ClientConfig, scheduled_start_utc_ms: i64) {
        // // Desync the clients a bit
        // let mut rng = rand::thread_rng();
        // let scheduled_start_utc_ms = scheduled_start_utc_ms + rng.gen_range(1..100);
        let now = Utc::now();
        let milliseconds_until_sync = scheduled_start_utc_ms - now.timestamp_millis();
        config.sync_time = Some(milliseconds_until_sync);
        if milliseconds_until_sync > 0 {
            tokio::time::sleep(Duration::from_millis(milliseconds_until_sync as u64)).await;
        } else {
            warn!("Started after synchronization point!");
        }
    }

    fn save_results(&self) -> Result<(), std::io::Error> {
        self.client_data.save_summary(self.config.clone())?;
        self.client_data
            .to_csv(self.config.output_filepath.clone())?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct RequestInterval {
    duration_sec: u64,
    requests_per_sec: u64,
    read_ratio: f64,
}

impl RequestInterval {
    fn get_interval_duration(self) -> Duration {
        Duration::from_secs(self.duration_sec)
    }

    fn get_request_delay(self) -> Duration {
        if self.requests_per_sec == 0 {
            return Duration::from_secs(999999);
        }
        let delay_ms = 1000 / self.requests_per_sec;
        assert!(delay_ms != 0);
        Duration::from_millis(delay_ms)
    }
}
