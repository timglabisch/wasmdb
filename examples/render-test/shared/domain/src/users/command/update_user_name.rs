use database::Database;
use rpc_command::rpc_command;
use sql_engine::storage::Uuid;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

#[rpc_command]
pub struct UpdateUserName {
    #[ts(type = "string")]
    pub id: Uuid,
    pub name: String,
}

impl Command for UpdateUserName {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        sql!(
            "UPDATE users SET name = {name} WHERE users.id = {id}",
            id = self.id,
            name = self.name,
        )
        .execute(db)
    }
}
