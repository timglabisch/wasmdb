use std::sync::Arc;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use borsh::BorshDeserialize;
use sync::command::Command;
use sync::protocol::{BatchCommandRequest, BatchCommandResponse, CommandResponse, Verdict};
use invoice_demo_commands::InvoiceCommand;

use crate::AppState;

/// POST /command — borsh-encoded batch of `InvoiceCommand`s; replies with a
/// borsh `BatchCommandResponse` (one verdict per request, order preserved).
pub async fn handle_command(
    State(state): State<Arc<AppState>>,
    body: Bytes,
) -> impl IntoResponse {
    let batch = match BatchCommandRequest::<InvoiceCommand>::try_from_slice(&body) {
        Ok(req) => req,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, e.to_string().into_bytes());
        }
    };

    let mut db = state.sync.db.lock().unwrap();

    let responses: Vec<CommandResponse> = batch
        .requests
        .into_iter()
        .map(|request| match request.command.execute(&mut db) {
            Ok(server_zset) => CommandResponse {
                stream_id: request.stream_id,
                seq_no: request.seq_no,
                verdict: Verdict::Confirmed { server_zset },
            },
            Err(e) => {
                eprintln!("[server] rejected seq={}: {}", request.seq_no.0, e);
                CommandResponse {
                    stream_id: request.stream_id,
                    seq_no: request.seq_no,
                    verdict: Verdict::Rejected { reason: e.to_string() },
                }
            }
        })
        .collect();

    let batch_response = BatchCommandResponse { responses };
    let bytes = borsh::to_vec(&batch_response).expect("serialize batch response");
    (StatusCode::OK, bytes)
}

/// POST /table-fetch — borsh `FetchRequest`, replies with borsh
/// `Vec<F::Row>` (the row type is implicit in `fetcher_id`).
pub async fn handle_table_fetch(
    State(state): State<Arc<AppState>>,
    body: Bytes,
) -> impl IntoResponse {
    match tables_storage::handle_fetch_bytes(&state.registry, &body, &state.ctx).await {
        Ok(bytes) => (StatusCode::OK, bytes),
        Err(e) => {
            eprintln!("[server] table-fetch failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string().into_bytes())
        }
    }
}
