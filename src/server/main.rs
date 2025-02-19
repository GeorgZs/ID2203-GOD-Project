use crate::{configs::OmniPaxosServerConfig, server::OmniPaxosServer};
use env_logger;
use std::{env, fs};
use toml;
use db::postgres_connection::PGConnection;
use db::repository::Repository;
use omnipaxos_kv::db;
use omnipaxos_kv::db::repository::DataSourceConnection;

mod configs;
mod database;
mod network;
mod server;

#[tokio::main]
pub async fn main() {
    /*env_logger::init();
    let config_file = match env::var("CONFIG_FILE") {
        Ok(file_path) => file_path,
        Err(_) => panic!("Requires CONFIG_FILE environment variable"),
    };
    let config_string = fs::read_to_string(config_file).unwrap();
    let server_config: OmniPaxosServerConfig = match toml::from_str(&config_string) {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };
    let mut server = OmniPaxosServer::new(server_config).await;
    server.run().await;*/
    println!("Hello Mihhail!!");
    // Connect to db here
    let mut pg_con = PGConnection::new().await;
    let rep = Repository::new(pg_con);


     Repository::query(&rep, "SELECT * FROM users;").await.expect("TODO: panic message");


    // Write simple insert

    // Query and print the data
}
