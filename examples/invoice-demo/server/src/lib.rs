use std::sync::Arc;
use axum::routing::post;
use sync_server::state::ServerState;
use invoice_demo_commands::InvoiceCommand;
use invoice_demo_tables_storage::{register_all, AppCtx};
use tables_storage::Registry;
use tower_http::services::ServeDir;

pub mod schema;
pub mod handler;

/// Wires the sync pipeline and the storage-table registry into a single
/// axum `State`. `handler::handle_command` only needs `sync`;
/// `handler::handle_table_fetch` needs `registry` + `ctx`.
pub struct AppState {
    pub sync: Arc<ServerState<InvoiceCommand>>,
    pub registry: Registry<AppCtx>,
    pub ctx: AppCtx,
}

/// Boots the invoice-demo HTTP server.
pub async fn run() {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://root:@127.0.0.1:4000/invoice_demo".to_string());

    let pool = sqlx::MySqlPool::connect(&database_url)
        .await
        .expect("connect to database");

    let mut registry = Registry::<AppCtx>::new();
    register_all(&mut registry);

    let state = Arc::new(AppState {
        sync: Arc::new(ServerState::<InvoiceCommand>::new(schema::make_db())),
        registry,
        ctx: AppCtx { pool },
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
