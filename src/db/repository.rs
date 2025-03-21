use std::future::Future;
use crate::common::ds::{DataSourceQueryType, TransactionId};

pub trait DataSourceConnection {
    fn new(host: String, port: String, db: String, user: String, password: String) -> impl Future<Output = Self>;
    fn read(&self, query_string: &str) -> impl Future<Output = Result<Option<Option<String>>, ()>>;
    fn write(&self, query_string: &str) -> impl Future<Output = Result<Option<Option<String>>, ()>>;
    fn write_in_tx(&self, tx_id: TransactionId, query_string: &str) -> impl Future<Output = Result<Option<Option<String>>, ()>>;
    fn commit_tx(&self, tx_id: TransactionId) -> impl Future<Output = Result<Option<Option<String>>, ()>>;
    fn begin_tx(&self, tx_id: TransactionId) -> impl Future<Output = Result<Option<Option<String>>, ()>>;
    fn rollback_tx(&self, tx_id: TransactionId) -> impl Future<Output = Result<Option<Option<String>>, ()>>;
    fn prepare_tx(&self, tx_id: TransactionId) -> impl Future<Output = Result<Option<Option<String>>, ()>>;
    fn rollback_prepared_tx(&self, tx_id: TransactionId) -> impl Future<Output = Result<Option<Option<String>>, ()>>;
}

pub struct Repository <T: DataSourceConnection> {
    connection: T
}

impl <T: DataSourceConnection> Repository<T> {
    pub fn new(connection: T) -> Self {
        Self { connection }
    }

    pub async fn query(&self, query_string: &str, query_type: DataSourceQueryType) -> Result<Option<Option<String>>, ()> {
        match query_type {
            DataSourceQueryType::READ => self.connection.read(query_string).await,
            DataSourceQueryType::INSERT => {
                self.connection.write(query_string).await
            },
            _ => panic!("Invalid query type")
            
        }
    }

    pub async fn query_in_tx(&self, tx_id: TransactionId, query_string: &str, query_type: DataSourceQueryType) -> Result<Option<Option<String>>, ()> {
        match query_type {
            DataSourceQueryType::READ => {
                self.connection.read(query_string).await
            },
            DataSourceQueryType::INSERT => {
                self.connection.write_in_tx(tx_id, query_string).await
            },
            _ => panic!("Invalid query type")

        }
    }

    pub async fn begin_tx(&self, tx_id: TransactionId) -> Result<Option<Option<String>>, ()> {
        self.connection.begin_tx(tx_id).await
    }

    pub async fn prepare_tx(&self, tx_id: TransactionId) -> Result<Option<Option<String>>, ()> {
        self.connection.prepare_tx(tx_id).await
    }

    pub async fn commit_tx(&self, tx_id: TransactionId) -> Result<Option<Option<String>>, ()> {
        self.connection.commit_tx(tx_id).await
    }
    pub async fn rollback_tx(&self, tx_id: TransactionId) -> Result<Option<Option<String>>, ()> {
        self.connection.rollback_tx(tx_id).await
    }

    pub async fn rollback_prepared_tx(&self, tx_id: TransactionId) -> Result<Option<Option<String>>, ()> {
        self.connection.rollback_prepared_tx(tx_id).await
    }
}

