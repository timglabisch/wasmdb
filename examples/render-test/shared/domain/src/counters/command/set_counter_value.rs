use database::Database;
use rpc_command::rpc_command;
use sql_engine::storage::Uuid;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

#[rpc_command]
pub struct SetCounterValue {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "number")]
    pub value: i64,
}

impl Command for SetCounterValue {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        sql!(
            "UPDATE counters SET value = {value} WHERE counters.id = {id}",
            id = self.id,
            value = self.value,
        )
        .execute(db)
    }
}
