use database::Database;
use rpc_command::rpc_command;
use sql_engine::storage::Uuid;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

/// Reassigns a message to a different room. Render-test scenario: both
/// the source and destination `<MessageList>` must re-render (one row
/// leaves, one enters); other rooms stay quiet.
#[rpc_command]
pub struct MoveMessage {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "string")]
    pub room_id: Uuid,
}

impl Command for MoveMessage {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        sql!(
            "UPDATE messages SET room_id = {room_id} WHERE messages.id = {id}",
            id = self.id,
            room_id = self.room_id,
        )
        .execute(db)
    }
}
