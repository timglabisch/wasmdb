use database::Database;
use rpc_command::rpc_command;
use sql_engine::storage::Uuid;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

#[rpc_command]
pub struct CreateCounter {
    #[ts(type = "string")]
    pub id: Uuid,
    pub label: String,
    #[ts(type = "number")]
    pub value: i64,
}

impl Command for CreateCounter {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        sql!(
            "INSERT INTO counters (id, label, value) VALUES ({self.id}, {self.label}, {self.value})"
        )
        .execute(db)
    }
}
