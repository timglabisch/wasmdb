//! Confirm-server for projection-demo.
//!
//! Like render-test's echo-server, every command is `Confirmed` with
//! `server_zset` derived from `client_zset` — no database, no
//! server-authoritative domain execution. The ONE addition: the appended
//! `ledger_log` row is returned with `committed = 1` (design §4.7). The
//! existing invert+apply reconcile finalizes the row from optimistic
//! (`committed = 0`) to committed, which
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
use projection_demo_domain::ledger::ledger_log::LedgerLog;
use projection_demo_domain::ProjectionDemoCommand;
use sql_engine::storage::{CellValue, ZSet};
use sql_engine::DbTable;
use sync::protocol::{
    BatchCommandRequest, BatchCommandResponse, CommandResponse, Verdict,
};
use tower_http::services::ServeDir;

/// Index of the `committed` column in `ledger_log`, resolved once from the
/// row schema so this stays correct if the column order ever changes.
fn committed_column_index() -> usize {
    LedgerLog::schema()
        .columns
        .iter()
        .position(|c| c.name == "committed")
        .expect("ledger_log has a `committed` column")
}

/// Return `client_zset` with every `ledger_log` row marked committed.
fn confirm_zset(mut zset: ZSet, committed_idx: usize) -> ZSet {
    for entry in &mut zset.entries {
        if entry.table == LedgerLog::TABLE {
            entry.row[committed_idx] = CellValue::I64(1);
        }
    }
    zset
}

async fn handle_command(body: Bytes) -> impl IntoResponse {
    let batch = match BatchCommandRequest::<ProjectionDemoCommand>::try_from_slice(&body) {
        Ok(req) => req,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string().into_bytes()),
    };

    let committed_idx = committed_column_index();
    let responses: Vec<CommandResponse> = batch
        .requests
        .into_iter()
        .map(|r| CommandResponse {
            stream_id: r.stream_id,
            seq_no: r.seq_no,
            verdict: Verdict::Confirmed {
                server_zset: confirm_zset(r.client_zset, committed_idx),
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

    let addr = "0.0.0.0:3126";
    eprintln!("[projection-demo-server] listening on http://localhost:3126");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
