use sqlx::postgres::PgPoolOptions;
use std::env;
use dotenv::dotenv;
use sqlx::query;
use crate::db::repository::DataSourceConnection;

pub struct PGConnection {
    pool: sqlx::PgPool
}

impl DataSourceConnection for PGConnection {
    async fn new() -> Self {
        dotenv().ok();

        // let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let postgres_user = env::var("POSTGRES_USER").expect("POSTGRES_USER must be set");
        let postgres_password = env::var("POSTGRES_PASSWORD").expect("POSTGRES_PASSWORD must be set");

        // let host = "db"; //should be localhost if not docker
        // let port = "5432"; //should be 5431 if not docker

        let host = "localhost";
        let port = "5431"; 

        let database_url = format!("postgres://{postgres_user}:{postgres_password}@{host}:{port}/postgres"); //for docker, if not docker use localhost

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url).await.unwrap();

        println!("DB connection created!");
        PGConnection{pool}
    }

    async fn read(&self, query_string: &str) {
        // todo!();
        println!("Reading from database!");
        let output = sqlx::query(query_string).fetch_all(&self.pool).await;

        match output {
            Ok(rows) => {
                for row in rows {
                    println!("{:?}", row);
                }
            },
            Err(e) => println!("Error executing query: {:?}", e),
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