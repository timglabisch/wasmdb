use database::Database;
use sql_engine::execute::Params;
use sync::command::CommandError;
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str};

pub fn run(
    db: &mut Database,
    id: i64, customer_id: i64, template_name: &str,
    interval_unit: &str, interval_value: i64, next_run: &str,
    status_template: &str, notes_template: &str,
) -> Result<ZSet, CommandError> {
    let params = Params::from([
        p_int("id", id), p_int("customer_id", customer_id),
        p_str("template_name", template_name),
        p_str("interval_unit", interval_unit),
        p_int("interval_value", interval_value),
        p_str("next_run", next_run),
        p_str("last_run", ""),
        p_int("enabled", 1),
        p_str("status_template", status_template),
        p_str("notes_template", notes_template),
    ]);
    execute_sql(db,
        "INSERT INTO recurring_invoices (id, customer_id, template_name, interval_unit, interval_value, next_run, last_run, enabled, status_template, notes_template) \
         VALUES (:id, :customer_id, :template_name, :interval_unit, :interval_value, :next_run, :last_run, :enabled, :status_template, :notes_template)",
        params)
}
