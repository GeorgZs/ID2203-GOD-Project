use omnipaxos_kv::common::ds::DataSourceCommand;
use std::collections::HashMap;
use omnipaxos_kv::db::postgres_connection::PGConnection;
use omnipaxos_kv::db::postgres_parser::PGParser;
use omnipaxos_kv::db::query_parser::{Parse, QueryParser};
use omnipaxos_kv::db::repository::{DataSourceConnection, Repository};

use crate::configs::DBConfig;

pub struct Database {
    db: Repository<PGConnection>,
    parser: QueryParser<PGParser>,
}

impl Database {
    pub async fn new(db_config: DBConfig) -> Self {
        Self {
            db: Repository::new(PGConnection::new(db_config.host, db_config.port, db_config.db, db_config.user, db_config.password).await),
            parser: QueryParser::new(PGParser::new())
        }
    }

    pub async fn handle_command(&mut self, command: DataSourceCommand) -> Option<Option<String>> {
        //TODO!!
        Repository::query(&self.db, self.parser.parse_dso(command).as_str(), "write").await.expect("TODO: panic message");

        /*match command {
            DataSourceCommand::Put(key, value) => {
                self.db.insert(key, value);
                None
            }
            KVCommand::Delete(key) => {
                self.db.remove(&key);
                None
            }
            KVCommand::Get(key) => Some(self.db.get(&key).map(|v| v.clone())),
        }*/
        None
    }
}
