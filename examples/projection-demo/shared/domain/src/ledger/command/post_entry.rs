use database::Database;
use rpc_command::{payload_json, rpc_command};
use sql_engine::storage::{CellValue, Uuid};
use sync::append::{append_row, next_seq};
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use tables::ProjectionLog;

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
    /// is the command's job: a provisional per-partition `seq`,
    /// `committed = 0` (off-chain until the server confirms), the event as
    /// the JSON payload the fold decodes back. The same append could come
    /// from any other trigger — it is an effect, not the command's identity.
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        let payload = payload_json(&EntryPosted { amount_cents: self.amount_cents })
            .map_err(CommandError::ExecutionFailed)?;
        let partition = CellValue::from(self.account.clone());
        let seq = next_seq::<LedgerLog>(db, LedgerLog::PARTITION_COLUMN, &partition)?;
        append_row(
            db,
            LedgerLog {
                command_id: self.id,
                account: self.account.clone(),
                seq,
                committed: 0,
                payload,
            },
        )
    }
}
