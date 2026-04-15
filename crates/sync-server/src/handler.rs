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
/// Receives a borsh-encoded BatchCommandRequest, executes all commands in order,
/// and returns a borsh-encoded BatchCommandResponse.
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

    let mut db = state.db.lock().unwrap();

    let responses: Vec<CommandResponse> = batch
        .requests
        .into_iter()
        .map(|request| match request.command.execute(&mut db) {
            Ok(server_zset) => CommandResponse {
                stream_id: request.stream_id,
                seq_no: request.seq_no,
                verdict: Verdict::Confirmed { server_zset },
            },
            Err(e) => CommandResponse {
                stream_id: request.stream_id,
                seq_no: request.seq_no,
                verdict: Verdict::Rejected {
                    reason: e.to_string(),
                },
            },
        })
        .collect();

    let batch_response = BatchCommandResponse { responses };
    let bytes = borsh::to_vec(&batch_response).expect("failed to serialize batch response");
    (StatusCode::OK, bytes)
}
