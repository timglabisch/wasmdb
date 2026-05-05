use database::Database;
use rpc_command::rpc_command;
use sql_engine::storage::Uuid;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

#[rpc_command]
pub struct AddMessage {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "string")]
    pub room_id: Uuid,
    #[ts(type = "string")]
    pub author_user_id: Uuid,
    pub body: String,
    #[client_default = "new Date().toISOString()"]
    pub created_at: String,
}

impl Command for AddMessage {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        sql!(
            "INSERT INTO messages (id, room_id, author_user_id, body, created_at) \
             VALUES ({self.id}, {self.room_id}, {self.author_user_id}, {self.body}, {self.created_at})"
        )
        .execute(db)
    }
}
