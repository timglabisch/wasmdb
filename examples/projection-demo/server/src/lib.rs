//! Confirm-server for projection-demo.
//!
//! No database, no server-authoritative store: every command approves its
//! own delta through `ServerCommand::execute_server`, and the result is
//! broadcast back as `server_zset`. `PostEntry` approves by echoing the
//! client's delta with the `ledger_log` row flipped to `committed = 1`
//! (design §4.7); the client's invert+apply reconcile then finalizes the row
//! from optimistic (`committed = 0`) to committed, which
//!
//!   1. lets `BalanceFold`'s committed-frontier memo advance (the fold no
//!      longer re-applies confirmed rows), and
//!   2. makes the optimistic → committed transition visible in the UI.
//!
//! The provisional `seq` is kept as authoritative — correct for a single
//! client (each account's `next_seq` already yields 0, 1, 2, …). A real
//! multi-client server would assign the authoritative `seq` here.

use axum::body::Bytes;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use borsh::BorshDeserialize;
use projection_demo_domain::{ProjectionDemoCommand, ServerCommand};
use sync::protocol::{
    BatchCommandRequest, BatchCommandResponse, CommandResponse, Verdict,
};
use tower_http::services::ServeDir;

async fn handle_command(body: Bytes) -> impl IntoResponse {
    let batch = match BatchCommandRequest::<ProjectionDemoCommand>::try_from_slice(&body) {
        Ok(req) => req,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string().into_bytes()),
    };

    let responses: Vec<CommandResponse> = batch
        .requests
        .into_iter()
        .map(|r| {
            let verdict = match r.command.execute_server(&r.client_zset) {
                Ok(server_zset) => Verdict::Confirmed { server_zset },
                Err(e) => Verdict::Rejected { reason: e.to_string() },
            };
            CommandResponse {
                stream_id: r.stream_id,
                seq_no: r.seq_no,
                verdict,
            }
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

    let addr = "0.0.0.0:3126";
    eprintln!("[projection-demo-server] listening on http://localhost:3126");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
