use std::collections::HashMap;
use std::sync::Arc;
use deadpool_postgres::{Manager, ManagerConfig, Object, Pool, RecyclingMethod};
use log::{debug, error, info};
use serde_json::Value;
use tokio::sync::Mutex;
use crate::db::repository::DataSourceConnection;
use tokio_postgres::{NoTls, Row};
use crate::common::ds::TransactionId;

pub struct PGConnection {
    pool: Pool,
    transaction_connections: Arc<Mutex<HashMap<String, Object>>>
}

impl DataSourceConnection for PGConnection {
    async fn new(host: String, port: String, db: String, user: String, password: String) -> Self {
        let mut config = tokio_postgres::Config::new();
        config.dbname(db);
        config.user(user);
        config.password(password);
        config.port(port.parse().unwrap());
        config.host(host);

        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };

        // Create the connection pool
        let mgr = Manager::from_config(config, NoTls, mgr_config);
        let pool = Pool::builder(mgr).max_size(16).build().unwrap();
        PGConnection{pool, transaction_connections: Arc::new(Mutex::new(HashMap::new()))}
    }

    // TODO ADD TX_READ
    async fn read(&self, query_string: &str) -> Result<Option<Option<String>>, ()> {
        let client = self.pool.get().await.unwrap();
        let stmt = client.prepare(query_string).await.unwrap();
        let output = client.query(&stmt, &[]).await;

        match output {
            Ok(rows) => {
                match pg_rows_to_json(rows).await {
                    Ok(json) => {Ok(Some(Some(json)))},
                    Err(_) => {Err(())}
                }
            },
            Err(e) => {
                debug!("Error executing query: {:?}", e);
                Err(())
            }
        }
    }
    async fn write(&self, query_string: &str) -> Result<Option<Option<String>>, ()> {
        let client = self.pool.get().await.unwrap();
        let stmt = client.prepare(query_string).await.unwrap();
        let output = client.query(&stmt, &[]).await;

        match output {
            Ok(_) => {},
            Err(e) => debug!("Error executing query: {:?}", e),
        }
        Ok(None)
    }
    async fn write_in_tx(&self, tx_id: TransactionId, query_string: &str) -> Result<Option<Option<String>>, ()> {
        let conns = Arc::clone(&self.transaction_connections);
        let mut connections = conns.lock().await;
        if let None = connections.get(&tx_id) {
            let cl = self.pool.get().await.unwrap();
            connections.insert(tx_id.clone(), cl);
        }
        let conn = connections.get_mut(&tx_id).unwrap();
        let stmt = conn.prepare(query_string).await.unwrap();
        let result = conn.query(&stmt, &[]).await;
        match result {
            Ok(_) => {Ok(None)}
            Err(e) => {
                debug!("Error executing query: {:?}", e);
                Err(())
            }
        }
    }

    async fn begin_tx(&self, tx_id: TransactionId) -> Result<Option<Option<String>>, ()> {
        //send begin statement to initiate transaction
        self.write_in_tx(tx_id, "BEGIN;").await
    }

    async fn prepare_tx(&self, tx_id: TransactionId) -> Result<Option<Option<String>>, ()> {
        let query_string = format!("PREPARE TRANSACTION '{}';", tx_id);
        self.write_in_tx(tx_id, &*query_string).await
    }

    async fn commit_tx(&self, tx_id: TransactionId) -> Result<Option<Option<String>>, ()> {
        let query_string = format!("COMMIT PREPARED '{}';", tx_id);
        let res = self.write_in_tx(tx_id.clone(), &*query_string).await;
        let conns_cl = self.transaction_connections.clone();
        let mut connections = conns_cl.lock().await;
        connections.remove(&tx_id);
        res
    }

    async fn rollback_tx(&self, tx_id: TransactionId) -> Result<Option<Option<String>>, ()> {
        let query_string = format!("ROLLBACK;");
        let res = self.write_in_tx(tx_id.clone(), &*query_string).await;
        let conns_cl = self.transaction_connections.clone();
        let mut connections = conns_cl.lock().await;
        connections.remove(&tx_id);
        res

    }

    async fn rollback_prepared_tx(&self, tx_id: TransactionId) -> Result<Option<Option<String>>, ()> {
        let query_string = format!("ROLLBACK PREPARED '{}';", tx_id);
        let res = self.write_in_tx(tx_id.clone(), &*query_string).await;
        let conns_cl = self.transaction_connections.clone();
        let mut connections = conns_cl.lock().await;
        connections.remove(&tx_id);
        res
    }
    

}

async fn pg_rows_to_json(rows: Vec<Row>) -> Result<String, Box<dyn std::error::Error>> {
    let mut json_rows = Vec::new();

    for row in rows {
        let mut json_obj = serde_json::Map::new();

        // For each column, extract data and convert to JSON
        let column_count = row.columns().len();
        for i in 0..column_count {
            let column_name = row.columns()[i].name();
            let column_type = row.columns()[i].type_();

            let json_value = match *column_type {
                postgres_types::Type::TEXT | postgres_types::Type::VARCHAR => row.get::<_, String>(i).into(),
                postgres_types::Type::INT2 => row.get::<_, i16>(i).into(),
                postgres_types::Type::INT4 => row.get::<_, i32>(i).into(),
                postgres_types::Type::INT8 => row.get::<_, i64>(i).into(),
                postgres_types::Type::FLOAT4 | postgres_types::Type::FLOAT8 => row.get::<_, f64>(i).into(),
                postgres_types::Type::BOOL => row.get::<_, bool>(i).into(),
                _ => Value::Null,
            };

            json_obj.insert(column_name.to_string(), json_value);
        }

        json_rows.push(Value::Object(json_obj));
    }

    let json_string = serde_json::to_string(&json_rows)?;

    Ok(json_string)
}