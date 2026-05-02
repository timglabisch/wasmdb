use sql_engine::storage::Uuid;
use database::Database;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use rpc_command::rpc_command;
use crate::command_helpers::SqlStmtExt;

#[rpc_command]
pub struct DeleteContact {
    #[ts(type = "string")]
    pub id: Uuid,
}

impl Command for DeleteContact {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        sql!("DELETE FROM contacts WHERE contacts.id = {id}", id = self.id).execute(db)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, ModelTrait, QueryFilter};
    use sync_server_mysql::ServerCommand;

    use crate::contacts::contact_server::entity as contact_entity;
    use crate::shared::DEMO_TENANT_ID;

    #[async_trait]
    impl ServerCommand for DeleteContact {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let model = contact_entity::Entity::find()
                .filter(contact_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(contact_entity::Column::Id.eq(self.id.0.to_vec()))
                .one(tx).await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "load contact {}: {e}", self.id,
                )))?
                .ok_or_else(|| CommandError::ExecutionFailed(format!(
                    "contact {} not found", self.id,
                )))?;
            model.delete(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                "DELETE contact {}: {e}", self.id,
            )))?;
            Ok(client_zset.clone())
        }
    }
}
