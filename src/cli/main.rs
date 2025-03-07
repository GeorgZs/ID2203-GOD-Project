use clap::Parser;
use rand::distributions::{Alphanumeric, DistString};
use omnipaxos_kv::common::{ds::{DataSourceCommand, DataSourceQueryType, QueryParams}, messages::{ClientMessage, ConsistencyLevel}, utils::get_node_addr};
use crate::{network::Network};

#[derive(Debug)]
enum PortLookup{
    S1 = 8001,
    S2 = 8002,
    S3 = 8003
  }

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(short, long, default_value_t = 1)]
    node: u8,

    /// Number of times to greet
    #[arg(short, long, default_value_t = String::from("local"))]
    consistency: String,

    #[arg(short, long)]
    action: String,
}

fn main() {
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

    let consistency_level: ConsistencyLevel = match args.consistency.as_str() {
        "local" => ConsistencyLevel::Local,
        "leader" => ConsistencyLevel::Leader,
        "linearizable" => ConsistencyLevel::Linearizable,
        _ => ConsistencyLevel::Local,
    };

    let client_message = ClientMessage::Read(unique_identifier, consistency_level, ds_command);

    // March statement to a node, match it to a 

    let address = match args.node {
        1 => format!("s1:{:?}",PortLookup::S1),
        2 => format!("s2:{:?}",PortLookup::S2),
        3 => format!("s3:{:?}",PortLookup::S3),
        _ => format!("s1:{:?}",PortLookup::S1),
      };

      let address = match args.node {
        1 => get_node_addr(&String::from("empty"), 1, true),
        2 => get_node_addr(&String::from("empty"), 2, true),
        3 => get_node_addr(&String::from("empty"), 3, true),
        _ => get_node_addr(&String::from("empty"), 1, true),
      };

      let network = Netwo::new(
        &String::from("empty"),
        vec![args.node],
        true,
        100
      ).await;




    
    
}