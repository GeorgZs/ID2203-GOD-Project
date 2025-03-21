use god_db::common::ds::{DataSourceCommand, TransactionId};
use god_db::db::postgres_connection::PGConnection;
use god_db::db::postgres_parser::PGParser;
use god_db::db::query_parser::{Parse, QueryParser};
use god_db::db::repository::{DataSourceConnection, Repository};

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

    pub async fn handle_command(&mut self, command: DataSourceCommand) -> Result<Option<Option<String>>, ()> {
        let query_type = command.query_type.clone();
        let tx_id_opt = command.tx_id.clone();
        match tx_id_opt {
            None => {
                Repository::query(&self.db, self.parser.parse_dso(command).as_str(), query_type).await
            }
            Some(tx_id_str) => {
                Repository::query_in_tx(&self.db, tx_id_str, self.parser.parse_dso(command).as_str(), query_type).await
            }
        }
    }

    pub async fn begin_tx(&self, tx_id: TransactionId) -> Result<Option<Option<String>>, ()> {
        Repository::begin_tx(&self.db, tx_id).await
    }

    pub async fn prepare_tx(&self, tx_id: TransactionId) -> Result<Option<Option<String>>, ()> {
        Repository::prepare_tx(&self.db, tx_id).await
    }

    pub async fn commit_tx(&self, tx_id: TransactionId) -> Result<Option<Option<String>>, ()> {
        Repository::commit_tx(&self.db, tx_id).await
    }

    pub async fn rollback_tx(&self, tx_id: TransactionId) -> Result<Option<Option<String>>, ()> {
        Repository::rollback_tx(&self.db, tx_id).await
    }

    pub async fn rollback_prepared_tx(&self, tx_id: TransactionId) -> Result<Option<Option<String>>, ()> {
        Repository::rollback_prepared_tx(&self.db, tx_id).await
    }
}
