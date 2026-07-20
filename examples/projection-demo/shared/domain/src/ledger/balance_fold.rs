use database_projection::{Out, RenderCtx};
use tables::ProjectionLog;
use tables_storage::projection;

use super::balance::Balance;
use super::command::post_entry::PostEntry;
use super::ledger_log::LedgerLog;

/// The fold state for one account's ledger. The projection is implemented
/// ON its state type (the cqrs-es Aggregate idiom): `apply` replays one
/// log row into the running state, `render` projects the accumulated
/// state into the derived `balance` row. The engine folds an account's
/// committed prefix once and memoizes it (design §9.3); only new rows
/// and the pending tail are re-applied on later changes.
#[derive(Default, Clone)]
pub struct BalanceFold {
    account: String,
    balance_cents: i64,
    entries: i64,
}

#[projection(outputs(Balance))]
impl BalanceFold {
    /// Replay one ledger event. The row's `payload` is the RPC form of
    /// the `PostEntry` that produced it — decode it back and accumulate.
    fn apply(&mut self, row: &LedgerLog) -> Result<(), String> {
        let entry: PostEntry = row.decode()?;
        self.account = entry.account;
        self.balance_cents += entry.amount_cents;
        self.entries += 1;
        Ok(())
    }

    /// Project the folded state into the derived read model.
    fn render(&self, _ctx: &RenderCtx<'_>, out: &mut Out) -> Result<(), String> {
        out.emit(Balance {
            account: self.account.clone(),
            balance_cents: self.balance_cents,
            entries: self.entries,
        });
        Ok(())
    }
}
