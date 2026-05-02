use async_trait::async_trait;
use sea_orm::DatabaseTransaction;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

/// Server-side counterpart of [`Command`]. Every command implements
/// `execute_server` to run its writes inside a SeaORM `DatabaseTransaction`
/// — this is the hook point for authoritative checks (permissions, cross-row
/// invariants, cascaded writes, …) that can't be enforced from the client.
///
/// There is deliberately no default body and no blanket
/// `impl<C: Command> ServerCommand for C`: requiring an explicit impl per
/// command means new variants can't silently fall back to a generic policy
/// — adding one forces the author to write (and review) the server-side
/// logic and its auth checks.
#[async_trait]
pub trait ServerCommand: Command {
    async fn execute_server(
        &self,
        tx: &DatabaseTransaction,
        client_zset: &ZSet,
    ) -> Result<ZSet, CommandError>;
}
