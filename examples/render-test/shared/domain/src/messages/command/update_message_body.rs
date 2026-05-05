use database::Database;
use rpc_command::rpc_command;
use sql_engine::storage::Uuid;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

#[rpc_command]
pub struct UpdateMessageBody {
    #[ts(type = "string")]
    pub id: Uuid,
    pub body: String,
}

impl Command for UpdateMessageBody {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        sql!(
            "UPDATE messages SET body = {body} WHERE messages.id = {id}",
            id = self.id,
            body = self.body,
        )
        .execute(db)
    }
}
