use serde::{Deserialize, Serialize};
use sql_engine::storage::Uuid;
use tables_storage::projection_row;

/// The append-only event log for account entries. `#[projection_row]`
/// declares only the identity — `command_id` (PK) and the `account`
/// partition — and generates the two-parent-link bookkeeping columns
/// `client_parent_id`, `server_parent_id` and `payload` (design §11). The
/// payload holds the domain *event* ([`EntryPosted`]), not the command that
/// produced it. A row is committed once the server has set its
/// `server_parent_id`.
///
/// One partition = one account's independently-ordered event stream.
#[projection_row]
pub struct LedgerLog {
    pub command_id: Uuid,
    pub account: String,
}

/// The domain event stored in a log row's `payload`: one posted entry's
/// signed amount (`+` deposit, `−` withdrawal).
///
/// Deliberately distinct from the `PostEntry` *command*. A command is a
/// request — it may arrive over RPC (as here), an HTTP API, an MCP tool.
/// The event is the fact that request appends to the log. The fold reads
/// this; it never sees the command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryPosted {
    pub amount_cents: i64,
}
