use database::Database;
use rpc_command::rpc_command;
use sql_engine::storage::Uuid;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

#[rpc_command]
pub struct UpdateMessageAuthor {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "string")]
    pub author_user_id: Uuid,
}

impl Command for UpdateMessageAuthor {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        sql!(
            "UPDATE messages SET author_user_id = {author_user_id} WHERE messages.id = {id}",
            id = self.id,
            author_user_id = self.author_user_id,
        )
        .execute(db)
    }
}
