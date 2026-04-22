use borsh::{BorshDeserialize, BorshSerialize};
use database::Database;
use crate::zset::ZSet;

/// Business command that produces a `ZSet` from the current client-side
/// `Database`. The client runs this optimistically to update its local state
/// before the server has seen the command; the resulting `ZSet` is shipped
/// in `CommandRequest.client_zset` so peers can re-apply the same delta.
///
/// The server-side counterpart is `ServerCommand` in backend-specific crates
/// like `sync-server-mysql` — each command implements it separately to run
/// its authoritative SQL (and any permission checks) directly against the
/// backing store.
///
/// The method is synchronous because `Database` is in-memory.
pub trait Command:
    BorshSerialize + BorshDeserialize + Clone + std::fmt::Debug + Send + Sync + 'static
{
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError>;
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub enum CommandError {
    ExecutionFailed(String),
}

impl std::fmt::Display for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommandError::ExecutionFailed(msg) => write!(f, "command execution failed: {msg}"),
        }
    }
}

impl std::error::Error for CommandError {}
