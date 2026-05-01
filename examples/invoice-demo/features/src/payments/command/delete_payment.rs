use sql_engine::storage::Uuid;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use rpc_command::rpc_command;
use crate::command_helpers::{execute_sql, p_uuid};
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
        let params = Params::from([p_uuid("id", &self.id)]);
        execute_sql(db, "DELETE FROM payments WHERE payments.id = :id", params)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter};
    use sync_server_mysql::ServerCommand;

    use crate::payments::payment_server::entity as payment_entity;

    #[async_trait]
    impl ServerCommand for DeletePayment {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            payment_entity::Entity::delete_many()
                .filter(payment_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(payment_entity::Column::Id.eq(self.id.0.to_vec()))
                .exec(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE payment {}: {e}", self.id,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
