use database::Database;
use sql_engine::execute::Params;
use sync::command::CommandError;
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int};

/// Cascades recurring_positions + recurring_invoice atomically.
pub fn run(db: &mut Database, id: i64) -> Result<ZSet, CommandError> {
    let mut acc = ZSet::new();
    let p = Params::from([p_int("rid", id)]);
    acc.extend(execute_sql(db,
        "DELETE FROM recurring_positions WHERE recurring_id = :rid", p)?);
    let p = Params::from([p_int("id", id)]);
    acc.extend(execute_sql(db,
        "DELETE FROM recurring_invoices WHERE id = :id", p)?);
    Ok(acc)
}
