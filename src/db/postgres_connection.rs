use sqlx::postgres::PgPoolOptions;
use std::env;
use dotenv::dotenv;
use crate::db::repository::DataSourceConnection;

pub struct PGConnection {
    pool: sqlx::PgPool
}

impl DataSourceConnection for PGConnection {
    async fn new() -> Self {
        dotenv().ok();

        // let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let database_url = "postgres://myuser:mypassword@localhost/rust_db";

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url).await.unwrap();

        PGConnection{pool}
    }

    fn execute_query(&self) {
        // todo!();
        println!("Hello, world!");
    }
}

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