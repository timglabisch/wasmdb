use database::Database;
use rpc_command::rpc_command;
use sql_engine::storage::Uuid;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

#[rpc_command]
pub struct CreateUser {
    #[ts(type = "string")]
    pub id: Uuid,
    pub name: String,
    pub status: String,
}

impl Command for CreateUser {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        sql!(
            "INSERT INTO users (id, name, status) VALUES ({self.id}, {self.name}, {self.status})"
        )
        .execute(db)
    }
}
