use clap::Parser;
use rand::distributions::{Alphanumeric, DistString};
use omnipaxos_kv::common::{ds::{Command, DataSourceCommand, DataSourceQueryType, QueryParams}, messages::{ClientMessage, ConsistencyLevel}};
use omnipaxos_kv::common::messages::{ServerMessage};
use crate::network::Network;

mod network;

const NETWORK_BATCH_SIZE: usize = 100;
const LOCAL_DEPLOYMENT: bool = true;
#[derive(Debug)]

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 1)]
    node: u64,

    #[arg(short, long, default_value_t = String::from("local"))]
    consistency: String,

    #[arg(short, long)]
    action: String,
}

#[tokio::main]
pub async fn main() {
    let args = Args::parse();


    let unique_identifier =Alphanumeric.sample_string(&mut rand::thread_rng(), 16);

    let ds_command = DataSourceCommand {
        query_type: DataSourceQueryType::READ,
        data_source_object: None,
        query_params: Some(QueryParams {
            table_name: String::from("users"),
            select_all: true,
            select_columns: None,
        }),
    };

    let command = Command {
      client_id: 1,
      coordinator_id: 1,
      id: 1,
      ds_cmd: ds_command
    };

    let consistency_level: ConsistencyLevel = match args.consistency.as_str() {
        "local" => ConsistencyLevel::Local,
        "leader" => ConsistencyLevel::Leader,
        "linearizable" => ConsistencyLevel::Linearizable,
        _ => ConsistencyLevel::Local,
    };

    let client_message = ClientMessage::Read(unique_identifier, consistency_level, command);

    // March statement to a node, match it to a

    let network_result = Network::new(
      String::from("empty"),
      vec![1,2,3],
      LOCAL_DEPLOYMENT,
      NETWORK_BATCH_SIZE
    ).await;

    print!("Before match");

    match network_result {
      mut network => {
        println!("Network created");
        network.send(1, client_message).await;

        match network.server_messages.recv().await {
          Some(ServerMessage::ReadResponse(_, _, opt_res)) => {
              // Self::wait_until_sync_time(&mut self.config, start_time).await;
              println!("Read response {:?}", opt_res);
          }
          _ => panic!("Error waiting for start signal"),
        }
      }
    }


}