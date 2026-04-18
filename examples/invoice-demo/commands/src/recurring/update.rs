use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use database::Database;
use sql_engine::execute::Params;
use sync::command::CommandError;
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct UpdateRecurring {
    pub id: i64,
    pub template_name: String,
    pub interval_unit: String,
    pub interval_value: i64,
    pub next_run: String,
    pub enabled: i64,
    pub status_template: String,
    pub notes_template: String,
}

impl UpdateRecurring {
    pub fn execute(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        let params = Params::from([
            p_int("id", self.id),
            p_str("template_name", &self.template_name),
            p_str("interval_unit", &self.interval_unit),
            p_int("interval_value", self.interval_value),
            p_str("next_run", &self.next_run),
            p_int("enabled", self.enabled),
            p_str("status_template", &self.status_template),
            p_str("notes_template", &self.notes_template),
        ]);
        execute_sql(db,
            "UPDATE recurring_invoices SET template_name = :template_name, interval_unit = :interval_unit, interval_value = :interval_value, next_run = :next_run, enabled = :enabled, status_template = :status_template, notes_template = :notes_template WHERE recurring_invoices.id = :id",
            params)
    }
}
