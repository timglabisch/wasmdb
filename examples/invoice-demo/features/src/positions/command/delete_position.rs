use sql_engine::storage::Uuid;
use database::Database;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use rpc_command::rpc_command;
use crate::command_helpers::execute_stmt;
use crate::shared::DEMO_TENANT_ID;

#[rpc_command]
pub struct DeletePosition {
    #[ts(type = "string")]
    pub id: Uuid,
}

impl Command for DeletePosition {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        execute_stmt(db, sql!("DELETE FROM positions WHERE positions.id = {id}", id = self.id))
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, ModelTrait, QueryFilter};
    use sync_server_mysql::ServerCommand;

    use crate::positions::position_server::entity as position_entity;

    #[async_trait]
    impl ServerCommand for DeletePosition {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let model = position_entity::Entity::find()
                .filter(position_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(position_entity::Column::Id.eq(self.id.0.to_vec()))
                .one(tx).await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "load position {}: {e}", self.id,
                )))?
                .ok_or_else(|| CommandError::ExecutionFailed(format!(
                    "position {} not found", self.id,
                )))?;
            model.delete(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                "DELETE position {}: {e}", self.id,
            )))?;
            Ok(client_zset.clone())
        }
    }
}
