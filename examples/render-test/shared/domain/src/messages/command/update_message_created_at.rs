use database::Database;
use rpc_command::rpc_command;
use sql_engine::storage::Uuid;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

#[rpc_command]
pub struct UpdateMessageCreatedAt {
    #[ts(type = "string")]
    pub id: Uuid,
    pub created_at: String,
}

impl Command for UpdateMessageCreatedAt {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        sql!(
            "UPDATE messages SET created_at = {created_at} WHERE messages.id = {id}",
            id = self.id,
            created_at = self.created_at,
        )
        .execute(db)
    }
}
