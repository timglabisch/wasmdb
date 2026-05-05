use database::Database;
use rpc_command::rpc_command;
use sql_engine::storage::Uuid;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

#[rpc_command]
pub struct UpdateCounterLabel {
    #[ts(type = "string")]
    pub id: Uuid,
    pub label: String,
}

impl Command for UpdateCounterLabel {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        sql!(
            "UPDATE counters SET label = {label} WHERE counters.id = {id}",
            id = self.id,
            label = self.label,
        )
        .execute(db)
    }
}
