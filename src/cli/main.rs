use clap::Parser;
use rand::distributions::{Alphanumeric, DistString};
use omnipaxos_kv::common::{ds::{DataSourceCommand, DataSourceQueryType, QueryParams}, messages::{ClientMessage, ConsistencyLevel}};
use omnipaxos_kv::common::messages::{RequestIdentifier, ServerMessage};
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
        tx_id: None,
        query_type: DataSourceQueryType::READ,
        data_source_object: None,
        query_params: Some(QueryParams {
            table_name: String::from("food"),
            select_all: true,
            select_columns: None,
        }),
    };

    let consistency_level: ConsistencyLevel = match args.consistency.as_str() {
        "local" => ConsistencyLevel::Local,
        "leader" => ConsistencyLevel::Leader,
        "linearizable" => ConsistencyLevel::Linearizable,
        _ => ConsistencyLevel::Local,
    };

    let client_message = ClientMessage::Read(unique_identifier.clone(), consistency_level, ds_command);

    // March statement to a node, match it to a

    let network_result = Network::new(
        unique_identifier as RequestIdentifier,
        String::from("empty"),
        vec![args.node],
        LOCAL_DEPLOYMENT,
        NETWORK_BATCH_SIZE
    ).await;

    match network_result {
      mut network => {
        network.send(args.node, client_message).await;

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