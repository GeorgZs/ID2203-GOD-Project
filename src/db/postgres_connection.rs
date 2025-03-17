use std::collections::HashMap;
use std::sync::Arc;
use deadpool_postgres::{Manager, ManagerConfig, Object, Pool, RecyclingMethod};
use dotenv::dotenv;
use log::info;
use serde_json::Value;
use tokio::sync::Mutex;
use crate::db::repository::DataSourceConnection;
use tokio_postgres::{NoTls, Row};

pub struct PGConnection {
    pool: Pool,
    transaction_connections: Arc<Mutex<HashMap<String, Test>>>
}

struct Test {
    counter: usize,
    conn: Object
}

impl DataSourceConnection for PGConnection {
    async fn new(host: String, port: String, db: String, user: String, password: String) -> Self {
        dotenv().ok();

        // let host = "db"; //should be localhost if not docker
        // let port = "5432"; //should be 5431 if not docker

        // let host = "localhost";
        // let port = "5431"; 

        //let database_url = format!("postgres://{user}:{password}@{host}:{port}/{db}"); //for docker, if not docker use localhost

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

        println!("DB connection created!");
        PGConnection{pool, transaction_connections: Arc::new(Mutex::new(HashMap::new()))}
    }

    // TODO ADD TX_READ
    async fn read(&self, query_string: &str) -> Option<Option<String>> {
        let client = self.pool.get().await.unwrap();
        let stmt = client.prepare(query_string).await.unwrap();
        let output = client.query(&stmt, &[]).await;

        match output {
            Ok(rows) => {
                match pg_rows_to_json(rows).await {
                    Ok(json) => {Some(Some(json))},
                    Err(_) => {Some(None)}
                }
            },
            Err(e) => {
                println!("Error executing query: {:?}", e);
                Some(None)
            }
        }
    }
    async fn write(&self, query_string: &str) {
        let client = self.pool.get().await.unwrap();
        let stmt = client.prepare(query_string).await.unwrap();
        let output = client.query(&stmt, &[]).await;

        match output {
            Ok(_) => println!("Write successful!"),
            Err(e) => println!("Error executing query: {:?}", e),
        }
    }
    async fn write_in_tx(&self, tx_id: String, query_string: &str) {
        let conns = Arc::clone(&self.transaction_connections);
        let mut connections = conns.lock().await;
        if let None = connections.get(&tx_id) {
            info!("DB connection doesn't exist: creating it: {:?}", tx_id);
            let cl = self.pool.get().await.unwrap();
            connections.insert(tx_id.clone(), Test{counter: 0, conn: cl});
        }
        let test = connections.get_mut(&tx_id).unwrap();
        let stmt = test.conn.prepare(query_string).await.unwrap();
        let _ = test.conn.query(&stmt, &[]).await;
        test.counter += 1;
        info!("Writing in tx: {:?}", tx_id);
        // TODO REMOVE IN ACTUAL COMMIT
        if test.counter == 3 {
            info!("Committing tx: {:?}", tx_id);
            connections.remove(&tx_id);
        }
    }

    async fn commit_tx(&self, tx_id: String) {
        let conns = Arc::clone(&self.transaction_connections);
        let mut connections = conns.lock().await;
        connections.remove(&tx_id);
        // TODO!! SEND COMMIT MESSAGE AS WELL;
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


// pub async fn get_user(pool: &sqlx::PgPool, user_id: i32) -> Result<User, sqlx::Error> {
//     let user = sqlx::query_as::<_, User>("SELECT * FROM users WHERE id = $1")
//         .bind(user_id)
//         .fetch_one(pool)
//         .await?;
//     Ok(user)
// }

// fn execute_query() -> Any {
//     pub async fn create_user(pool: &sqlx::PgPool, name: &str, email: &str) -> Result<(), sqlx::Error> {
    // sqlx::query("INSERT INTO users (name, email) VALUES ($1, $2)")
    //         .bind(name)
    //         .bind(email)
    //         .execute(pool)
    //         .await?;
    //     Ok(())
// }

// #[derive(sqlx::FromRow)]
// pub struct User {
//     pub id: i32,
//     pub name: String,
//     pub email: String,
// }

// pub async fn create_user(pool: &sqlx::PgPool, name: &str, email: &str) -> Result<(), sqlx::Error> {
//     sqlx::query("INSERT INTO users (name, email) VALUES ($1, $2)")
//         .bind(name)
//         .bind(email)
//         .execute(pool)
//         .await?;
//     Ok(())
// }

// pub async fn get_user(pool: &sqlx::PgPool, user_id: i32) -> Result<User, sqlx::Error> {
//     let user = sqlx::query_as::<_, User>("SELECT * FROM users WHERE id = $1")
//         .bind(user_id)
//         .fetch_one(pool)
//         .await?;
//     Ok(user)
// }