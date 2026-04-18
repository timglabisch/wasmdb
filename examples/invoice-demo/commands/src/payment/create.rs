use database::Database;
use sql_engine::execute::Params;
use sync::command::CommandError;
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str};

pub fn run(
    db: &mut Database,
    id: i64, invoice_id: i64, amount: i64, paid_at: &str,
    method: &str, reference: &str, note: &str,
) -> Result<ZSet, CommandError> {
    let params = Params::from([
        p_int("id", id), p_int("invoice_id", invoice_id),
        p_int("amount", amount), p_str("paid_at", paid_at),
        p_str("method", method), p_str("reference", reference),
        p_str("note", note),
    ]);
    execute_sql(db,
        "INSERT INTO payments (id, invoice_id, amount, paid_at, method, reference, note) \
         VALUES (:id, :invoice_id, :amount, :paid_at, :method, :reference, :note)",
        params)
}
