use database::Database;
use sql_engine::execute::Params;
use sync::command::CommandError;
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str};

pub fn run(
    db: &mut Database,
    id: i64, timestamp: &str,
    entity_type: &str, entity_id: i64,
    action: &str, actor: &str, detail: &str,
) -> Result<ZSet, CommandError> {
    let params = Params::from([
        p_int("id", id),
        p_str("timestamp", timestamp),
        p_str("entity_type", entity_type),
        p_int("entity_id", entity_id),
        p_str("action", action),
        p_str("actor", actor),
        p_str("detail", detail),
    ]);
    execute_sql(db,
        "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
         VALUES (:id, :timestamp, :entity_type, :entity_id, :action, :actor, :detail)",
        params)
}
