//! Render-test umbrella crate. Minimal but reactivity-divers schema:
//! `users`, `rooms` (FK→users), `messages` (FK→rooms+users), `counters`.
//! Each table targets a specific re-render pattern verified by the
//! Playwright integration tests.
//!
//! No `feature = "server"`: the echo-server doesn't dispatch
//! `ServerCommand`, so this crate stays wasm-friendly with no
//! MySQL/SeaORM transitive deps.

pub mod counters;
pub mod messages;
pub mod rooms;
pub mod users;

// ============================================================
// Command wire enum
// ============================================================

use borsh::{BorshDeserialize, BorshSerialize};
use database::Database;
use rpc_command::rpc_command_enum;
use serde::{Deserialize, Serialize};
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use ts_rs::TS;

use counters::command::{
    create_counter::CreateCounter, set_counter_value::SetCounterValue,
    update_counter_label::UpdateCounterLabel,
};
use messages::command::{
    add_message::AddMessage, delete_message::DeleteMessage,
    move_message::MoveMessage, update_message_author::UpdateMessageAuthor,
    update_message_body::UpdateMessageBody,
    update_message_created_at::UpdateMessageCreatedAt,
};
use rooms::command::{
    create_room::CreateRoom, rename_room::RenameRoom, transfer_room::TransferRoom,
};
use users::command::{
    create_user::CreateUser, delete_user::DeleteUser,
    update_user_name::UpdateUserName, update_user_status::UpdateUserStatus,
};

/// Wire-format enum. Variant order is API-stable (Borsh index).
#[rpc_command_enum]
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
#[serde(tag = "type")]
#[ts(export)]
pub enum RenderTestCommand {
    CreateUser(CreateUser),
    UpdateUserName(UpdateUserName),
    UpdateUserStatus(UpdateUserStatus),
    DeleteUser(DeleteUser),

    CreateRoom(CreateRoom),
    RenameRoom(RenameRoom),
    TransferRoom(TransferRoom),

    AddMessage(AddMessage),
    DeleteMessage(DeleteMessage),
    MoveMessage(MoveMessage),
    UpdateMessageAuthor(UpdateMessageAuthor),
    UpdateMessageBody(UpdateMessageBody),
    UpdateMessageCreatedAt(UpdateMessageCreatedAt),

    CreateCounter(CreateCounter),
    SetCounterValue(SetCounterValue),
    UpdateCounterLabel(UpdateCounterLabel),
}

impl Command for RenderTestCommand {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        match self {
            RenderTestCommand::CreateUser(c) => c.execute_optimistic(db),
            RenderTestCommand::UpdateUserName(c) => c.execute_optimistic(db),
            RenderTestCommand::UpdateUserStatus(c) => c.execute_optimistic(db),
            RenderTestCommand::DeleteUser(c) => c.execute_optimistic(db),
            RenderTestCommand::CreateRoom(c) => c.execute_optimistic(db),
            RenderTestCommand::RenameRoom(c) => c.execute_optimistic(db),
            RenderTestCommand::TransferRoom(c) => c.execute_optimistic(db),
            RenderTestCommand::AddMessage(c) => c.execute_optimistic(db),
            RenderTestCommand::DeleteMessage(c) => c.execute_optimistic(db),
            RenderTestCommand::MoveMessage(c) => c.execute_optimistic(db),
            RenderTestCommand::UpdateMessageAuthor(c) => c.execute_optimistic(db),
            RenderTestCommand::UpdateMessageBody(c) => c.execute_optimistic(db),
            RenderTestCommand::UpdateMessageCreatedAt(c) => c.execute_optimistic(db),
            RenderTestCommand::CreateCounter(c) => c.execute_optimistic(db),
            RenderTestCommand::SetCounterValue(c) => c.execute_optimistic(db),
            RenderTestCommand::UpdateCounterLabel(c) => c.execute_optimistic(db),
        }
    }
}
