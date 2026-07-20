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
//! execute_server` approves the client's delta in-memory by stamping the
//! authoritative `server_parent_id` (design §11), advancing a per-account
//! chain head — and recording the committed row — in [`ServerLog`]. That
//! same store answers fetch-by-PK requests, so a client that joined a
//! partition late can backfill ancestors it never saw (§11.4 gap-repair).
//! No SQL backend, no `feature = "server"`. The crate stays wasm-friendly.

pub mod ledger;

// ============================================================
// Command wire enum
// ============================================================

use std::collections::HashMap;

use borsh::{BorshDeserialize, BorshSerialize};
use database::Database;
use rpc_command::{payload_json, rpc_command_enum};
use serde::{Deserialize, Serialize};
use sql_engine::storage::{CellValue, Uuid};
use sql_engine::DbTable;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use tables::ROOT_PARENT;
use ts_rs::TS;

use ledger::command::post_entry::PostEntry;
use ledger::ledger_log::{EntryPosted, LedgerLog};

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

/// The confirm-server's authoritative state (design §11.5, extended for
/// §11.4 gap-repair). Two pieces, still no SQL backend:
///
/// - `heads`: per account, the PK of the current committed-chain head —
///   the frontier each new confirmation links onto (the ordering state
///   §11.5 describes).
/// - `rows`: every committed row by `command_id`. Stufe 1 didn't need this
///   (the client already held every row it posted), but gap-repair does: a
///   client that joins a partition another writer already advanced receives
///   a `server_parent_id` it never fetched, and backfills the missing
///   ancestors via [`Self::fetch`]. This is the store the fetch-by-PK
///   endpoint answers from.
///
/// `PostEntry::execute_server` consults `heads` to assign the authoritative
/// `server_parent_id`, then records the stamped row here.
#[derive(Default)]
pub struct ServerLog {
    heads: HashMap<String, Uuid>,
    rows: HashMap<Uuid, Vec<CellValue>>,
    /// Monotonic counter for [`Self::foreign_write`] so each simulated
    /// out-of-band entry gets a fresh, collision-free PK.
    foreign_seq: u32,
}

impl ServerLog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Link `command_id` onto `account`'s chain: return the current head —
    /// the `server_parent_id` to stamp, `ROOT_PARENT` for the account's
    /// first commit — then advance the head to `command_id`. Because the
    /// server picks the order here, out-of-order confirmations chain in the
    /// order this is *called*, not the order the client posted.
    pub fn link(&mut self, account: &str, command_id: Uuid) -> Uuid {
        let parent = self.heads.get(account).copied().unwrap_or(ROOT_PARENT);
        self.heads.insert(account.to_string(), command_id);
        parent
    }

    /// Retain a confirmed `ledger_log` row (schema-order cells) by its PK so
    /// the fetch-by-PK endpoint can serve it during a client's backward
    /// refetch.
    pub fn record(&mut self, command_id: Uuid, row: Vec<CellValue>) {
        self.rows.insert(command_id, row);
    }

    /// Answer a [`FetchRowsRequest`](sync::protocol::FetchRowsRequest): the
    /// stored `ledger_log` rows for `ids`, as a `+1` ZSet ready to apply.
    /// Unknown ids are simply absent — the client stops when the answer
    /// carries no new ancestor.
    pub fn fetch(&self, ids: &[Uuid]) -> ZSet {
        let mut zset = ZSet::new();
        for id in ids {
            if let Some(row) = self.rows.get(id) {
                zset.insert(LedgerLog::TABLE.into(), row.clone());
            }
        }
        zset
    }

    /// The current committed-chain head of every partition — the tip
    /// `command_id`s a fresh client bootstraps from (answers a
    /// [`HeadsRequest`](sync::protocol::HeadsRequest)). Sorted for a
    /// deterministic response.
    pub fn heads(&self) -> Vec<Uuid> {
        let mut ids: Vec<Uuid> = self.heads.values().copied().collect();
        ids.sort();
        ids
    }

    /// Simulate *another writer* advancing a partition out-of-band: append
    /// `count` committed entries to `account`, each linked onto the running
    /// head, and return their `command_id`s. No client is in the loop, so a
    /// client that later syncs (or posts to this account) receives a
    /// `server_parent_id` it never fetched and must gap-repair (design
    /// §11.4). Fresh `0xcf…`-prefixed PKs (distinct from carol's `0xca…`
    /// seed and the client's random v4 ids) via the monotonic counter.
    pub fn foreign_write(&mut self, account: &str, count: u32) -> Vec<Uuid> {
        // A small recognizable pattern so the injected activity reads as a
        // real ledger rather than noise.
        const PATTERN: [i64; 3] = [1500, -400, 900];
        let mut ids = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let n = self.foreign_seq;
            self.foreign_seq += 1;
            let mut b = [0u8; 16];
            b[0] = 0xcf;
            b[14] = (n >> 8) as u8;
            b[15] = n as u8;
            let command_id = Uuid(b);
            let amount_cents = PATTERN[(n as usize) % PATTERN.len()];
            let parent = self.link(account, command_id);
            let payload =
                payload_json(&EntryPosted { amount_cents }).expect("foreign payload serializes");
            let row = LedgerLog {
                command_id,
                account: account.to_string(),
                client_parent_id: parent,
                server_parent_id: Some(parent),
                payload,
            };
            self.record(command_id, row.into_cells());
            ids.push(command_id);
        }
        ids
    }

    /// Seed a pre-existing committed chain for `account` — as if another
    /// writer had already advanced this partition before the current client
    /// joined. Each `(command_id, amount_cents)` becomes a committed row
    /// linked onto the previous (`ROOT` first), with `client_parent_id`
    /// mirroring `server_parent_id` (that writer's optimism held, so no
    /// drift on the seeded rows). Records every row and advances the head,
    /// so a fresh client posting here gets an unknown `server_parent_id` and
    /// must gap-repair (design §11.4).
    pub fn seed_chain(&mut self, account: &str, entries: &[(Uuid, i64)]) {
        for &(command_id, amount_cents) in entries {
            let parent = self.link(account, command_id);
            let payload = payload_json(&EntryPosted { amount_cents })
                .expect("seed payload serializes");
            let row = LedgerLog {
                command_id,
                account: account.to_string(),
                client_parent_id: parent,
                server_parent_id: Some(parent),
                payload,
            };
            self.record(command_id, row.into_cells());
        }
    }
}

/// Server-side, DB-less counterpart of [`Command`]. The confirm-server
/// owns no SQL store: a command *approves* the client's optimistic
/// `client_zset` — stamping the authoritative `server_parent_id` from the
/// [`ServerLog`] frontier, and recording the committed row so it can be
/// refetched — and returns the delta to broadcast back to peers.
///
/// This is the in-memory analog of `sync-server-mysql`'s `ServerCommand`
/// (which runs authoritative SQL in a `DatabaseTransaction`). The demo needs
/// no SQL backend, so the trait lives here — a `ZSet -> ZSet` transform over
/// a tiny in-memory log, wasm-friendly, no `feature = "server"`.
pub trait ServerCommand {
    fn execute_server(
        &self,
        client_zset: &ZSet,
        log: &mut ServerLog,
    ) -> Result<ZSet, CommandError>;
}

impl ServerCommand for ProjectionDemoCommand {
    fn execute_server(
        &self,
        client_zset: &ZSet,
        log: &mut ServerLog,
    ) -> Result<ZSet, CommandError> {
        match self {
            ProjectionDemoCommand::PostEntry(c) => c.execute_server(client_zset, log),
        }
    }
}
