use std::sync::Arc;

use axum::routing::post;
use invoice_demo_features::{register_all, AppCtx};
use tables_storage::Registry;
use tower_http::services::ServeDir;

pub mod handler;

/// Stateless invoice-demo server: TiDB is the single source of truth. The
/// `pool` inside `ctx` is cloned into the per-request `MysqlRunner`.
pub struct AppState {
    pub registry: Registry<AppCtx>,
    pub ctx: AppCtx,
}

pub async fn run() {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://root:@127.0.0.1:4000/invoice_demo".to_string());

    let pool = sqlx::MySqlPool::connect(&database_url)
        .await
        .expect("connect to database");

    let db = sea_orm::SqlxMySqlConnector::from_sqlx_mysql_pool(pool.clone());

    let mut registry = Registry::<AppCtx>::new();
    register_all(&mut registry);

    let state = Arc::new(AppState {
        registry,
        ctx: AppCtx { pool, db },
    });

    let static_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../frontend/dist");

    let app = axum::Router::new()
        .route("/command", post(handler::handle_command))
        .route("/table-fetch", post(handler::handle_table_fetch))
        .nest_service("/", ServeDir::new(&static_dir))
        .with_state(state);

    let addr = "0.0.0.0:3124";
    eprintln!("[invoice-server] listening on http://localhost:3124 (db: {database_url})");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
