use sql_engine::storage::Uuid;
use database::Database;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use rpc_command::rpc_command;
use crate::command_helpers::SqlStmtExt;
use crate::shared::DEMO_TENANT_ID;

#[rpc_command]
pub struct DeletePayment {
    #[ts(type = "string")]
    pub id: Uuid,
}

impl Command for DeletePayment {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        sql!("DELETE FROM payments WHERE payments.id = {id}", id = self.id).execute(db)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, ModelTrait, QueryFilter};
    use sync_server_mysql::ServerCommand;

    use crate::payments::payment_server::entity as payment_entity;

    #[async_trait]
    impl ServerCommand for DeletePayment {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let model = payment_entity::Entity::find()
                .filter(payment_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(payment_entity::Column::Id.eq(self.id.0.to_vec()))
                .one(tx).await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "load payment {}: {e}", self.id,
                )))?
                .ok_or_else(|| CommandError::ExecutionFailed(format!(
                    "payment {} not found", self.id,
                )))?;
            model.delete(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                "DELETE payment {}: {e}", self.id,
            )))?;
            Ok(client_zset.clone())
        }
    }
}
