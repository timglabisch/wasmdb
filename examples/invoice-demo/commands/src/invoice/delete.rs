use database::Database;
use sql_engine::execute::Params;
use sync::command::CommandError;
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int};

/// Cascades positions + payments + invoice — all in one atomic ZSet.
pub fn run(db: &mut Database, id: i64) -> Result<ZSet, CommandError> {
    let mut acc = ZSet::new();
    let p = Params::from([p_int("iid", id)]);
    acc.extend(execute_sql(db,
        "DELETE FROM payments WHERE invoice_id = :iid", p)?);
    let p = Params::from([p_int("iid", id)]);
    acc.extend(execute_sql(db,
        "DELETE FROM positions WHERE invoice_id = :iid", p)?);
    let p = Params::from([p_int("id", id)]);
    acc.extend(execute_sql(db,
        "DELETE FROM invoices WHERE id = :id", p)?);
    Ok(acc)
}
