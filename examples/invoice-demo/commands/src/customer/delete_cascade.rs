use database::Database;
use sql_engine::execute::{Params, ParamValue};
use sync::command::CommandError;
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, read_i64_col};

pub fn run(db: &mut Database, id: i64) -> Result<ZSet, CommandError> {
    // Fan out: collect recurring_ids + invoice_ids, then delete in
    // dependency order, extending a single ZSet atomically.
    let recurring_ids = read_i64_col(db,
        "SELECT id FROM recurring_invoices WHERE customer_id = :cid",
        Params::from([p_int("cid", id)]))?;
    let invoice_ids = read_i64_col(db,
        "SELECT id FROM invoices WHERE customer_id = :cid",
        Params::from([p_int("cid", id)]))?;

    let mut acc = ZSet::new();

    if !recurring_ids.is_empty() {
        let p = Params::from([
            ("rids".into(), ParamValue::IntList(recurring_ids.clone())),
        ]);
        acc.extend(execute_sql(db,
            "DELETE FROM recurring_positions WHERE recurring_id IN (:rids)", p)?);
        let p = Params::from([
            ("rids".into(), ParamValue::IntList(recurring_ids)),
        ]);
        acc.extend(execute_sql(db,
            "DELETE FROM recurring_invoices WHERE id IN (:rids)", p)?);
    }

    if !invoice_ids.is_empty() {
        let p = Params::from([
            ("iids".into(), ParamValue::IntList(invoice_ids.clone())),
        ]);
        acc.extend(execute_sql(db,
            "DELETE FROM payments WHERE invoice_id IN (:iids)", p)?);
        let p = Params::from([
            ("iids".into(), ParamValue::IntList(invoice_ids.clone())),
        ]);
        acc.extend(execute_sql(db,
            "DELETE FROM positions WHERE invoice_id IN (:iids)", p)?);
        let p = Params::from([
            ("iids".into(), ParamValue::IntList(invoice_ids)),
        ]);
        acc.extend(execute_sql(db,
            "DELETE FROM invoices WHERE id IN (:iids)", p)?);
    }

    let p = Params::from([p_int("cid", id)]);
    acc.extend(execute_sql(db,
        "DELETE FROM sepa_mandates WHERE customer_id = :cid", p)?);
    let p = Params::from([p_int("cid", id)]);
    acc.extend(execute_sql(db,
        "DELETE FROM contacts WHERE customer_id = :cid", p)?);
    let p = Params::from([p_int("id", id)]);
    acc.extend(execute_sql(db,
        "DELETE FROM customers WHERE id = :id", p)?);

    Ok(acc)
}
