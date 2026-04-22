use std::collections::HashMap;

use async_trait::async_trait;
use sql_engine::schema::TableSchema;
use sql_engine::storage::ZSet;
use sqlx::{MySql, Transaction};
use sync::command::{Command, CommandError};

/// Server-side counterpart of [`Command`]. Every command must explicitly
/// implement `execute_server`: straight-CRUD variants delegate to
/// [`apply_zset`] to replay the client's optimistic `ZSet` verbatim,
/// authoritative variants (cross-row invariants, pre-image checks,
/// cascaded writes based on current DB state, …) talk to sqlx directly.
///
/// There is deliberately no default body and no blanket
/// `impl<C: Command> ServerCommand for C`: requiring an explicit impl per
/// command means new variants can't silently fall back to replay — adding
/// one forces the author to pick a server-side policy.
#[async_trait]
pub trait ServerCommand: Command {
    async fn execute_server(
        &self,
        tx: &mut Transaction<'static, MySql>,
        client_zset: &ZSet,
        schemas: &HashMap<String, TableSchema>,
    ) -> Result<ZSet, CommandError>;
}
