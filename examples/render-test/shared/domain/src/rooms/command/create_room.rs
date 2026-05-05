use database::Database;
use rpc_command::rpc_command;
use sql_engine::storage::Uuid;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

#[rpc_command]
pub struct CreateRoom {
    #[ts(type = "string")]
    pub id: Uuid,
    pub name: String,
    #[ts(type = "string")]
    pub owner_user_id: Uuid,
}

impl Command for CreateRoom {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        sql!(
            "INSERT INTO rooms (id, name, owner_user_id) \
             VALUES ({self.id}, {self.name}, {self.owner_user_id})"
        )
        .execute(db)
    }
}
