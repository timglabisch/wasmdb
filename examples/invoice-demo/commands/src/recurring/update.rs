use database::Database;
use sql_engine::execute::Params;
use sync::command::CommandError;
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str};

pub fn run(
    db: &mut Database,
    id: i64, template_name: &str,
    interval_unit: &str, interval_value: i64, next_run: &str, enabled: i64,
    status_template: &str, notes_template: &str,
) -> Result<ZSet, CommandError> {
    let params = Params::from([
        p_int("id", id),
        p_str("template_name", template_name),
        p_str("interval_unit", interval_unit),
        p_int("interval_value", interval_value),
        p_str("next_run", next_run),
        p_int("enabled", enabled),
        p_str("status_template", status_template),
        p_str("notes_template", notes_template),
    ]);
    execute_sql(db,
        "UPDATE recurring_invoices SET template_name = :template_name, interval_unit = :interval_unit, interval_value = :interval_value, next_run = :next_run, enabled = :enabled, status_template = :status_template, notes_template = :notes_template WHERE recurring_invoices.id = :id",
        params)
}
