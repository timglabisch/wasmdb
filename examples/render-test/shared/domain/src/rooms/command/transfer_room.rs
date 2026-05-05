use database::Database;
use rpc_command::rpc_command;
use sql_engine::storage::Uuid;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

/// Reassigns ownership. Render-test scenario: only `<RoomRow:id>` should
/// re-render. The previous owner's `<UserBadge>` MUST NOT (the user row
/// itself didn't change). The new owner's badge re-renders because the
/// `<RoomRow>` now reads a different user.
#[rpc_command]
pub struct TransferRoom {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "string")]
    pub owner_user_id: Uuid,
}

impl Command for TransferRoom {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        sql!(
            "UPDATE rooms SET owner_user_id = {owner_user_id} WHERE rooms.id = {id}",
            id = self.id,
            owner_user_id = self.owner_user_id,
        )
        .execute(db)
    }
}
