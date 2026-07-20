use rpc_command::rpc_command;
use sql_engine::storage::Uuid;

use super::super::ledger_log::LedgerLog;

/// Post one ledger entry for an account. `append_to = LedgerLog` makes the
/// command's entire optimistic effect a single append to the event log
/// (design §4.4): `execute_optimistic` fills the log row's `command_id`
/// from `id`, its partition from the `#[partition]` `account`, a
/// provisional `seq`, `committed = 0`, and `payload = ` this command as
/// JSON. `BalanceFold` decodes that payload back on every fold.
///
/// A signed `amount_cents` carries the direction: positive = deposit,
/// negative = withdrawal. One event shape, no discriminant needed — the
/// fold just sums.
#[rpc_command(append_to = LedgerLog)]
pub struct PostEntry {
    /// Client-generated command id → the log row's `command_id` PK.
    #[ts(type = "string")]
    pub id: Uuid,
    /// The account this entry belongs to → the log partition.
    #[partition]
    pub account: String,
    /// Signed amount in cents: `+` deposit, `−` withdrawal.
    #[ts(type = "number")]
    pub amount_cents: i64,
}
