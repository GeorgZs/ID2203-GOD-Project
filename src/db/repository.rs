//Define CRUD operations

use std::any::Any;

struct Repository {
    connection: DataSourceConnection
}

impl Repository {
    fn new(connection: DataSourceConnection) -> Self {
        connection: connection
    }

    fn query(q: &str) {
        connection::execute_query(&str);
    }
}
trait DataSourceConnection {
    fn connect_db();
    fn execute_query() -> Any;
}

fn initializer_connect() {
    // calls postgres_connection.rs
}

enum SqlCommand {
    INSERT(HashMap::<String, Any>)
}

fn write() {}
fn read() {}


