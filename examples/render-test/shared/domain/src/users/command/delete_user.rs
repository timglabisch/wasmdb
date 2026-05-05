use database::Database;
use rpc_command::rpc_command;
use sql_engine::storage::Uuid;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

#[rpc_command]
pub struct DeleteUser {
    #[ts(type = "string")]
    pub id: Uuid,
}

impl Command for DeleteUser {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        sql!("DELETE FROM users WHERE users.id = {id}", id = self.id).execute(db)
    }
}
