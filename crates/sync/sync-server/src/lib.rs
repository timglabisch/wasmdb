pub mod handler;
pub mod state;

use std::sync::Arc;
use axum::routing::post;
use sync::command::Command;
use crate::state::ServerState;

pub fn build_router<C>(state: Arc<ServerState<C>>) -> axum::Router
where
    C: Command,
{
    axum::Router::new()
        .route("/command", post(handler::handle_command::<C>))
        .with_state(state)
}
