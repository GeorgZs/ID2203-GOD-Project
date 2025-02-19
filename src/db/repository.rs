//Define CRUD operations
use std::future::Future;

pub trait DataSourceConnection {
    fn new() -> impl Future<Output = Self>;
    fn read(&self, query_string: &str) -> impl Future<Output = ()>;
}

pub struct Repository <T: DataSourceConnection> {
    connection: T
}

impl <T: DataSourceConnection> Repository<T> {
    pub fn new(connection: T) -> Self {
        Self { connection }
    }

    pub async fn query(&self, query_string: &str) -> Result<(), ()> {
        self.connection.read(query_string);
        Ok(())
    }
}


fn initializer_connect() {
    // calls postgres_connection.rs
}

// enum SqlCommand {
//     INSERT(HashMap::<String, Any>)
// }

fn write() {}
fn read() {}


