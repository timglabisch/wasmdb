use sql_engine::storage::Uuid;
use tables_storage::projection_row;

/// The append-only event log for account entries. `#[projection_row]`
/// declares only the identity — `command_id` (PK) and the `account`
/// partition — and generates the bookkeeping columns `seq`, `committed`,
/// and `payload` (the RPC form of the command as JSON). The projection
/// engine sources its fold from these rows; nothing writes it by hand.
///
/// One partition = one account's independently-ordered event stream.
#[projection_row]
pub struct LedgerLog {
    pub command_id: Uuid,
    pub account: String,
}
