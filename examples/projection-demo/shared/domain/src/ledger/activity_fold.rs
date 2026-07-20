use database_projection::{Out, RenderCtx};
use tables::ProjectionLog;
use tables_storage::dynamic_projection;

use super::account_activity::AccountActivity;
use super::ledger_log::{EntryPosted, LedgerLog};

/// Instance names are `['account', <account>]`: component 0 is a
/// namespace discriminator, component 1 binds the log's account column.
pub const ACTIVITY_PROJECTION_ID: &str = "activity";

/// The demand-driven counterpart to `BalanceFold` (design §12), written in
/// the same aggregate idiom: the struct is the fold state, `apply` replays
/// one log row, `render` projects the state into the derived row. The
/// difference is the lifecycle, not the shape — an instance folds ONE
/// account's rows and only exists while activated: activate materializes
/// on demand, the last deactivate retracts the row. Registered by hand in
/// the wasm crate (`register_dynamic`); codegen does not pick templates up.
#[derive(Default, Clone)]
pub struct ActivityFold {
    account: String,
    deposits: i64,
    withdrawals: i64,
    largest_cents: i64,
}

#[dynamic_projection(id = "activity", outputs(AccountActivity), bind(account = 1))]
impl ActivityFold {
    /// Replay one ledger event. The account comes from the row — every
    /// row the engine feeds in matches the footprint, so it equals the
    /// instance name's component 1.
    fn apply(&mut self, row: &LedgerLog) -> Result<(), String> {
        let event: EntryPosted = row.decode()?;
        self.account = row.account.clone();
        if event.amount_cents >= 0 {
            self.deposits += 1;
        } else {
            self.withdrawals += 1;
        }
        if event.amount_cents.abs() > self.largest_cents.abs() {
            self.largest_cents = event.amount_cents;
        }
        Ok(())
    }

    /// Project the folded state into the derived read model.
    fn render(&self, _ctx: &RenderCtx<'_>, out: &mut Out) -> Result<(), String> {
        out.emit(AccountActivity {
            account: self.account.clone(),
            deposits: self.deposits,
            withdrawals: self.withdrawals,
            largest_cents: self.largest_cents,
        });
        Ok(())
    }
}
