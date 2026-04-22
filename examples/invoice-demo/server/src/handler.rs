use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use borsh::BorshDeserialize;
use invoice_demo_commands::InvoiceCommand;
use sync::protocol::{BatchCommandRequest, BatchCommandResponse, CommandResponse, Verdict};
use sync_server_mysql::ServerCommand;

use crate::AppState;

/// POST /command — borsh-encoded batch of `InvoiceCommand`s. Each batch
/// runs inside a single TiDB transaction; the first error rolls back and
/// every subsequent command is `Rejected` with a batch-aborted reason.
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

    let mut tx = match state.ctx.pool.begin().await {
        Ok(t) => t,
        Err(e) => {
            eprintln!("[server] pool.begin failed: {e}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                e.to_string().into_bytes(),
            );
        }
    };

    let mut responses: Vec<CommandResponse> = Vec::with_capacity(batch.requests.len());
    let mut first_err: Option<String> = None;

    for request in batch.requests {
        if let Some(prev) = &first_err {
            responses.push(CommandResponse {
                stream_id: request.stream_id,
                seq_no: request.seq_no,
                verdict: Verdict::Rejected {
                    reason: format!("batch aborted: {prev}"),
                },
            });
            continue;
        }

        let verdict = match request
            .command
            .execute_server(&mut tx, &request.client_zset, &state.schemas)
            .await
        {
            Ok(server_zset) => Verdict::Confirmed { server_zset },
            Err(e) => {
                let msg = e.to_string();
                eprintln!("[server] rejected seq={}: {msg}", request.seq_no.0);
                first_err = Some(msg.clone());
                Verdict::Rejected { reason: msg }
            }
        };
        responses.push(CommandResponse {
            stream_id: request.stream_id,
            seq_no: request.seq_no,
            verdict,
        });
    }

    if first_err.is_some() {
        if let Err(e) = tx.rollback().await {
            eprintln!("[server] rollback failed: {e}");
        }
    } else if let Err(e) = tx.commit().await {
        eprintln!("[server] commit failed: {e}");
        let reason = format!("commit failed: {e}");
        responses = responses
            .into_iter()
            .map(|r| CommandResponse {
                stream_id: r.stream_id,
                seq_no: r.seq_no,
                verdict: Verdict::Rejected {
                    reason: reason.clone(),
                },
            })
            .collect();
    }

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
