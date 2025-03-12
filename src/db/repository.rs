use std::future::Future;
use crate::common::ds::DataSourceQueryType;

pub trait DataSourceConnection {
    fn new(host: String, port: String, db: String, user: String, password: String) -> impl Future<Output = Self>;
    fn read(&self, query_string: &str) -> impl Future<Output = Option<Option<String>>>;
    fn write(&self, query_string: &str) -> impl Future<Output = ()>;
}

pub struct Repository <T: DataSourceConnection> {
    connection: T
}

impl <T: DataSourceConnection> Repository<T> {
    pub fn new(connection: T) -> Self {
        Self { connection }
    }

    pub async fn query(&self, query_string: &str, query_type: DataSourceQueryType) -> Option<Option<String>> {
        match query_type {
            DataSourceQueryType::READ => self.connection.read(query_string).await,
            DataSourceQueryType::INSERT => {
                self.connection.write(query_string).await;
                None
            },
            _ => panic!("Invalid query type")
            
        }
    }
}

