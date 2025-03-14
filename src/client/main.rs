use client::{Client, ClientConfig};
use core::panic;
use env_logger;
use std::{env, fs};
use toml;

mod client;
mod data_collection;
mod network;

#[tokio::main]
pub async fn main() {
    env_logger::init();
    let config_file = match env::var("CONFIG_FILE") {
        Ok(file_path) => file_path,
        Err(_) => panic!("Requires CONFIG_FILE environment variable"),
    };
    let config_string = fs::read_to_string(config_file).unwrap();
    let client_config: ClientConfig = match toml::from_str(&config_string) {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };
    let mut client = Client::new(client_config).await;
    client.run().await;
}
