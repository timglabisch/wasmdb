use database::Database;
use rpc_command::rpc_command;
use sql_engine::storage::Uuid;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

#[rpc_command]
pub struct UpdateUserStatus {
    #[ts(type = "string")]
    pub id: Uuid,
    pub status: String,
}

impl Command for UpdateUserStatus {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        sql!(
            "UPDATE users SET status = {status} WHERE users.id = {id}",
            id = self.id,
            status = self.status,
        )
        .execute(db)
    }
}
