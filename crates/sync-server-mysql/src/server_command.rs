use std::collections::HashMap;

use async_trait::async_trait;
use sql_engine::schema::TableSchema;
use sql_engine::storage::ZSet;
use sqlx::{MySql, Transaction};
use sync::command::{Command, CommandError};

/// Server-side counterpart of [`Command`]. Every command implements
/// `execute_server` to run its SQL directly inside the sqlx transaction —
/// this is the hook point for authoritative checks (permissions, cross-row
/// invariants, cascaded writes, …) that can't be enforced from the client.
///
/// There is deliberately no default body and no blanket
/// `impl<C: Command> ServerCommand for C`: requiring an explicit impl per
/// command means new variants can't silently fall back to a generic policy
/// — adding one forces the author to write (and review) the server-side
/// SQL and its auth checks.
#[async_trait]
pub trait ServerCommand: Command {
    async fn execute_server(
        &self,
        tx: &mut Transaction<'static, MySql>,
        client_zset: &ZSet,
        schemas: &HashMap<String, TableSchema>,
    ) -> Result<ZSet, CommandError>;
}
