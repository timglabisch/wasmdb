use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use database::Database;
use sql_engine::execute::Params;
use sync::command::CommandError;
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct LogActivity {
    pub id: i64,
    pub timestamp: String,
    pub entity_type: String,
    pub entity_id: i64,
    pub action: String,
    pub actor: String,
    pub detail: String,
}

impl LogActivity {
    pub fn execute(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        let params = Params::from([
            p_int("id", self.id),
            p_str("timestamp", &self.timestamp),
            p_str("entity_type", &self.entity_type),
            p_int("entity_id", self.entity_id),
            p_str("action", &self.action),
            p_str("actor", &self.actor),
            p_str("detail", &self.detail),
        ]);
        execute_sql(db,
            "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
             VALUES (:id, :timestamp, :entity_type, :entity_id, :action, :actor, :detail)",
            params)
    }
}
