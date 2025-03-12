use sqlx::postgres::{PgPoolOptions, PgRow};
use dotenv::dotenv;
use serde_json::{Value};
use sqlx::{Column, Row};
use crate::db::repository::DataSourceConnection;

pub struct PGConnection {
    pool: sqlx::PgPool
}

impl DataSourceConnection for PGConnection {
    async fn new(host: String, port: String, db: String, user: String, password: String) -> Self {
        dotenv().ok();

        // let host = "db"; //should be localhost if not docker
        // let port = "5432"; //should be 5431 if not docker

        // let host = "localhost";
        // let port = "5431"; 

        let database_url = format!("postgres://{user}:{password}@{host}:{port}/{db}"); //for docker, if not docker use localhost

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url).await.unwrap();

        println!("DB connection created!");
        PGConnection{pool}
    }

    async fn read(&self, query_string: &str) -> Option<Option<String>> {
        let output = sqlx::query(query_string).fetch_all(&self.pool).await;

        match output {
            Ok(rows) => {
                match pg_rows_to_json(&*rows).await {
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
        // todo!();
        println!("Writing to database!");
        let output = sqlx::query(query_string).execute(&self.pool).await;

        match output {
            Ok(_) => println!("Write successful!"),
            Err(e) => println!("Error executing query: {:?}", e),
        }
    }
}


async fn pg_rows_to_json(rows: &[PgRow]) -> Result<String, serde_json::Error> {
    let mut json_rows = Vec::new();

    for row in rows {
        let mut json_obj = serde_json::Map::new();

        // For each column, extract data and convert to JSON
        let column_count = row.columns().len();
        for i in 0..column_count {
            let column_name = row.columns().get(i).unwrap().name().to_string();
            let value: Option<Value> = row.try_get(i).ok();

            // Insert into the JSON object
            json_obj.insert(column_name, value.unwrap_or(Value::Null));
        }

        json_rows.push(Value::Object(json_obj));
    }

    // Convert the `Vec<Value>` to a JSON string
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