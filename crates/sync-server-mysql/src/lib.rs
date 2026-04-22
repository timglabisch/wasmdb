//! MySQL/TiDB adapter for the sync server.
//!
//! Exposes the [`ServerCommand`] trait: every command type implements
//! `execute_server` to run its SQL directly inside a sqlx `Transaction`.
//! This is where authoritative server-side logic (permission checks,
//! cross-row invariants, cascade deletes, …) lives.

pub mod server_command;

pub use server_command::ServerCommand;
