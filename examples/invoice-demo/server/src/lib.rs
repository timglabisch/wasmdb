use std::sync::Arc;
use axum::routing::post;
use sync_server::state::ServerState;
use invoice_demo_commands::InvoiceCommand;
use tower_http::services::ServeDir;

pub mod schema;
pub mod handler;

/// Boots the invoice-demo HTTP server: POST /command for the sync protocol,
/// and a ServeDir for the built frontend at the site root.
pub async fn run() {
    let state = Arc::new(ServerState::<InvoiceCommand>::new(schema::make_db()));

    let static_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../frontend/dist");

    let app = axum::Router::new()
        .route("/command", post(handler::handle_command))
        .nest_service("/", ServeDir::new(&static_dir))
        .with_state(state);

    let addr = "0.0.0.0:3124";
    eprintln!("[invoice-server] listening on http://localhost:3124");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
