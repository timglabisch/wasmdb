use database::Database;
use rpc_command::rpc_command;
use sql_engine::execute::Params;
use sql_engine::storage::Uuid;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

use crate::command_helpers::{execute_sql, p_uuid};
use crate::shared::DEMO_TENANT_ID;

#[rpc_command]
pub struct DeleteCustomer {
    #[ts(type = "string")]
    pub id: Uuid,
}

impl Command for DeleteCustomer {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let params = Params::from([p_uuid("id", &self.id)]);
        execute_sql(db, "DELETE FROM customers WHERE customers.id = :id", params)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter};
    use sync_server_mysql::ServerCommand;

    use crate::customers::customer_server::entity as customer_entity;

    #[async_trait]
    impl ServerCommand for DeleteCustomer {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            customer_entity::Entity::delete_many()
                .filter(customer_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(customer_entity::Column::Id.eq(self.id.0.to_vec()))
                .exec(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE customer {}: {e}", self.id,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
