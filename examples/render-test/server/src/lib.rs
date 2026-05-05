use axum::body::Bytes;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use borsh::BorshDeserialize;
use render_test_domain::RenderTestCommand;
use sync::protocol::{
    BatchCommandRequest, BatchCommandResponse, CommandResponse, Verdict,
};
use tower_http::services::ServeDir;

/// Echo-server. Every command is `Confirmed` with `server_zset = client_zset`.
/// Reactivity, command-pipeline, and rollback paths still execute end-to-end —
/// only the persistence/server-authoritative side is stubbed out.
async fn handle_command(body: Bytes) -> impl IntoResponse {
    let batch = match BatchCommandRequest::<RenderTestCommand>::try_from_slice(&body) {
        Ok(req) => req,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string().into_bytes()),
    };

    let responses: Vec<CommandResponse> = batch
        .requests
        .into_iter()
        .map(|r| CommandResponse {
            stream_id: r.stream_id,
            seq_no: r.seq_no,
            verdict: Verdict::Confirmed {
                server_zset: r.client_zset,
            },
        })
        .collect();

    let bytes = borsh::to_vec(&BatchCommandResponse { responses })
        .expect("serialize batch response");
    (StatusCode::OK, bytes)
}

pub async fn run() {
    let static_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../frontend/apps/ui/dist");

    let app = axum::Router::new()
        .route("/command", post(handle_command))
        .nest_service("/", ServeDir::new(&static_dir));

    let addr = "0.0.0.0:3125";
    eprintln!("[render-test-server] listening on http://localhost:3125");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
