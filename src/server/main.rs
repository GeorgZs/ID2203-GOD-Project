use crate::{configs::OmniPaxosServerConfig, server::OmniPaxosServer};
use env_logger;
use std::{env, fs};
use toml;

mod configs;
mod database;
mod network;
mod server;
mod omnipaxos_rsm;
mod transactions_rsm;
mod shard_rsm;
mod coordinator_rsm;

#[tokio::main]
pub async fn main() {
    env_logger::init();
    let config_file = match env::var("CONFIG_FILE") {
        Ok(file_path) => file_path,
        Err(_) => panic!("Requires CONFIG_FILE environment variable"),
    };
    println!("Config File: {}", config_file);
    let config_string = fs::read_to_string(config_file).unwrap();
    println!("Config String: {}", config_string);
    let server_config: OmniPaxosServerConfig = match toml::from_str(&config_string) {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };
    let mut server = OmniPaxosServer::new(server_config).await;
    server.run().await;

    println!("Hello Mihhail!!");
    // Connect to db here
    /*let pg_con = PGConnection::new().await;
    let rep = Repository::new(pg_con);

    Repository::query(&rep, "SELECT * FROM users;", "read").await.expect("TODO: panic message");

    Repository::query(&rep, "INSERT INTO users (name) VALUES ('Jeff'), ('Rob');", "write").await.expect("TODO: panic message");

    Repository::query(&rep, "SELECT * FROM users;", "read").await.expect("TODO: panic message");

    let pg_parser = PGParser::new();
    let parser = QueryParser::new(pg_parser);*/

    //There are two options with my implementation:
    //1. Pass a DataSourceObject to the parser, returning a query string (stores query in parser as well)
    // parser.parse_dso("insert", object);

    //2. Read the query string from the parser, containing past queries as a Vec<String>
    // let queries = parser.get_query_string();
    // for query in queries {
        // println!("{}", query);
    // }
    //The first option is more convenient and easy, but the second option is secure and allows for more control with concurrent queries
    
    // 1st => easy and read queries as they come
    // 2nd => secure and need to iterate or pop from list of queries

   
    // Write simple insert

    // Query and print the data
}
