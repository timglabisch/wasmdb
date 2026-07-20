//! projection-demo umbrella crate — an event-sourced account ledger that
//! showcases the projection engine end-to-end.
//!
//! The data flow the demo makes visible:
//!
//! ```text
//!   PostEntry command  ──append──▶  ledger_log  ──fold──▶  balance
//!   (Deposit/Withdraw)              (event log)  (BalanceFold) (derived)
//! ```
//!
//! - `ledger_log` (`#[projection_row]`) is the append-only event log:
//!   one row per posted entry, `command_id` PK, partitioned by `account`.
//!   A `PostEntry` command's whole optimistic effect is appending its own
//!   row (design §4.4) — no hand-written derivation.
//! - `BalanceFold` (`#[projection]`) folds each account's rows into a
//!   running balance and writes the derived `balance` table. The engine
//!   maintains it at the notify chokepoint, incrementally (design §9.3).
//!
//! No `feature = "server"`: the demo server confirms without executing
//! domain logic, so this crate stays wasm-friendly.

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
