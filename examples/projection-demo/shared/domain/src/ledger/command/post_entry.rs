use database::Database;
use rpc_command::{payload_json, rpc_command};
use sql_engine::storage::{CellValue, Uuid};
use sql_engine::DbTable;
use sync::append::{append_row, client_head};
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use tables::ProjectionLog;

use crate::{ServerCommand, ServerLog};

use super::super::ledger_log::{EntryPosted, LedgerLog};

/// Post one ledger entry for an account — a *request*. It arrives over
/// RPC here, but the same intent could come from an HTTP API or an MCP
/// tool. The command is not itself a log row: `execute_optimistic`
/// explicitly appends an [`EntryPosted`] event to `ledger_log`.
///
/// A signed `amount_cents` carries the direction: positive = deposit,
/// negative = withdrawal. One event shape, no discriminant needed — the
/// fold just sums.
#[rpc_command]
pub struct PostEntry {
    /// Client-generated command id → the log row's `command_id` PK.
    #[ts(type = "string")]
    pub id: Uuid,
    /// The account this entry belongs to → the log partition.
    pub account: String,
    /// Signed amount in cents: `+` deposit, `−` withdrawal.
    #[ts(type = "number")]
    pub amount_cents: i64,
}

impl Command for PostEntry {
    /// Append one `EntryPosted` event to the ledger log. Building the row
    /// is the command's job: `client_parent_id` = the account's current
    /// client-chain head (its optimistic predecessor), `server_parent_id =
    /// None` (off-chain until the server links it), the event as the JSON
    /// payload the fold decodes back (design §11). The same append could
    /// come from any other trigger — it is an effect, not the command's
    /// identity.
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        let payload = payload_json(&EntryPosted { amount_cents: self.amount_cents })
            .map_err(CommandError::ExecutionFailed)?;
        let partition = CellValue::from(self.account.clone());
        let client_parent_id =
            client_head::<LedgerLog>(db, LedgerLog::PARTITION_COLUMN, &partition)?;
        append_row(
            db,
            LedgerLog {
                command_id: self.id,
                account: self.account.clone(),
                client_parent_id,
                server_parent_id: None,
                payload,
            },
        )
    }
}

impl ServerCommand for PostEntry {
    /// Approve the append. The demo confirm-server holds a small in-memory
    /// log ([`ServerLog`]): echo the client's delta back with the
    /// `ledger_log` row's `server_parent_id` stamped to the account's
    /// current chain head (design §11.5) — `ROOT_PARENT` for the first
    /// commit — advance that head, and *record* the committed row so a later
    /// gap-repair can refetch it. The client's invert+apply reconcile then
    /// finalizes the optimistic row (`server_parent_id: None` → `Some(..)`),
    /// advancing the fold's committed frontier and flipping the UI from
    /// pending to confirmed. A per-row drift is then visible as
    /// `client_parent_id != server_parent_id`; if the stamped parent is one
    /// the client never fetched, its repair loop backfills it (§11.4).
    fn execute_server(
        &self,
        client_zset: &ZSet,
        log: &mut ServerLog,
    ) -> Result<ZSet, CommandError> {
        let server_parent = log.link(&self.account, self.id);
        let server_parent_idx = LedgerLog::schema()
            .columns
            .iter()
            .position(|c| c.name == "server_parent_id")
            .expect("ledger_log has a `server_parent_id` column");
        let mut zset = client_zset.clone();
        for entry in &mut zset.entries {
            if entry.table == LedgerLog::TABLE {
                entry.row[server_parent_idx] = CellValue::from(server_parent);
                log.record(self.id, entry.row.clone());
            }
        }
        Ok(zset)
    }
}
