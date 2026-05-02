use std::sync::Arc;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use borsh::BorshDeserialize;
use sync::command::Command;
use sync::protocol::{BatchCommandRequest, BatchCommandResponse, CommandResponse, Verdict};
use crate::state::ServerState;

/// POST /command
///
/// Receives a borsh-encoded `BatchCommandRequest`, applies every command's
/// `client_zset` to the in-memory `Database`, and echoes it back as
/// `server_zset`. The in-memory backend trusts the client's optimistic
/// delta — customization hooks live in backend-specific server crates
/// (e.g. `sync-server-mysql`).
pub async fn handle_command<C>(
    State(state): State<Arc<ServerState<C>>>,
    body: Bytes,
) -> impl IntoResponse
where
    C: Command,
{
    let batch = match BatchCommandRequest::<C>::try_from_slice(&body) {
        Ok(req) => req,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, e.to_string().into_bytes());
        }
    };

    let mut db = state.db.lock().await;

    let mut responses: Vec<CommandResponse> = Vec::with_capacity(batch.requests.len());
    for request in batch.requests {
        let verdict = match db.apply_zset(&request.client_zset) {
            Ok(()) => Verdict::Confirmed {
                server_zset: request.client_zset,
            },
            Err(e) => Verdict::Rejected { reason: e.to_string() },
        };
        responses.push(CommandResponse {
            stream_id: request.stream_id,
            seq_no: request.seq_no,
            verdict,
        });
    }

    let batch_response = BatchCommandResponse { responses };
    let bytes = borsh::to_vec(&batch_response).expect("failed to serialize batch response");
    (StatusCode::OK, bytes)
}
