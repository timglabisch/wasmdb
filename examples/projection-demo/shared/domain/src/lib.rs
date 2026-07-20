//! projection-demo umbrella crate — an event-sourced account ledger that
//! showcases the projection engine end-to-end.
//!
//! The data flow the demo makes visible:
//!
//! ```text
//!   PostEntry command  ──append──▶  ledger_log  ──fold──▶  balance
//!   (Deposit/Withdraw)   EntryPosted  (event log)  (BalanceFold) (derived)
//! ```
//!
//! - A `PostEntry` command is a *request*; its optimistic effect is to
//!   append one `EntryPosted` event to the log (its `execute_optimistic`
//!   builds the row directly via `sync::append`). The command is not the
//!   log row — the same append could be triggered by an HTTP API or an
//!   MCP tool. Appending is an effect, not identity.
//! - `ledger_log` (`#[projection_row]`) is the append-only event log:
//!   one row per posted entry, `command_id` PK, partitioned by `account`,
//!   the `EntryPosted` event in its payload.
//! - `BalanceFold` (`#[projection]`) folds each account's events into a
//!   running balance and writes the derived `balance` table. The engine
//!   maintains it at the notify chokepoint, incrementally (design §9.3).
//!
//! Server-side confirmation lives on the command too: `ServerCommand::
//! execute_server` approves the client's delta in-memory (a pure `ZSet`
//! transform, no SQL backend). No `feature = "server"` — the crate stays
//! wasm-friendly.

pub mod ledger;

// ============================================================
// Command wire enum
// ============================================================

use borsh::{BorshDeserialize, BorshSerialize};
use database::Database;
use rpc_command::rpc_command_enum;
use serde::{Deserialize, Serialize};
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use ts_rs::TS;

use ledger::command::post_entry::PostEntry;

/// Wire-format enum. A single append command today; the enum keeps the
/// door open for more event kinds without touching the transport.
#[rpc_command_enum]
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
#[serde(tag = "type")]
#[ts(export)]
pub enum ProjectionDemoCommand {
    PostEntry(PostEntry),
}

impl Command for ProjectionDemoCommand {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        match self {
            ProjectionDemoCommand::PostEntry(c) => c.execute_optimistic(db),
        }
    }
}

// ============================================================
// Server-side approval (in-memory, no store)
// ============================================================

/// Server-side, store-less counterpart of [`Command`]. The confirm-server
/// owns no database: a command *approves* the client's optimistic
/// `client_zset` and returns the delta to broadcast back to peers.
///
/// This is the in-memory analog of `sync-server-mysql`'s `ServerCommand`
/// (which runs authoritative SQL in a `DatabaseTransaction`). The demo needs
/// no SQL backend, so the trait lives here — a pure `ZSet -> ZSet` transform,
/// wasm-friendly, no `feature = "server"`.
pub trait ServerCommand {
    fn execute_server(&self, client_zset: &ZSet) -> Result<ZSet, CommandError>;
}

impl ServerCommand for ProjectionDemoCommand {
    fn execute_server(&self, client_zset: &ZSet) -> Result<ZSet, CommandError> {
        match self {
            ProjectionDemoCommand::PostEntry(c) => c.execute_server(client_zset),
        }
    }
}
