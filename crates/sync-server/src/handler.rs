use std::sync::Arc;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use borsh::BorshDeserialize;
use sync::command::Command;
use sync::protocol::{CommandRequest, CommandResponse, Verdict};
use crate::state::ServerState;

/// POST /command
/// Receives a borsh-encoded CommandRequest, executes the command,
/// and returns a borsh-encoded CommandResponse.
pub async fn handle_command<C>(
    State(state): State<Arc<ServerState<C>>>,
    body: Bytes,
) -> impl IntoResponse
where
    C: Command,
{
    let request = match CommandRequest::<C>::try_from_slice(&body) {
        Ok(req) => req,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, e.to_string().into_bytes());
        }
    };

    let mut db = state.db.lock().unwrap();

    let response = match request.command.execute(&mut db) {
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
    };

    let bytes = borsh::to_vec(&response).expect("failed to serialize response");
    (StatusCode::OK, bytes)
}
