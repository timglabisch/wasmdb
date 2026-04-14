use borsh::{BorshSerialize, BorshDeserialize};
use database::Database;
use crate::zset::ZSet;

pub trait Command: BorshSerialize + BorshDeserialize + Clone + std::fmt::Debug + Send + Sync + 'static {
    fn execute(&self, db: &mut Database) -> Result<ZSet, CommandError>;
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
