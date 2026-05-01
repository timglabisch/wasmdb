use sql_engine::storage::Uuid;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use rpc_command::rpc_command;
use crate::command_helpers::{execute_sql, p_uuid};

#[rpc_command]
pub struct DeleteSepaMandate {
    #[ts(type = "string")]
    pub id: Uuid,
}

impl Command for DeleteSepaMandate {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let params = Params::from([p_uuid("id", &self.id)]);
        execute_sql(db, "DELETE FROM sepa_mandates WHERE sepa_mandates.id = :id", params)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter};
    use sync_server_mysql::ServerCommand;

    use crate::sepa_mandates::sepa_mandate_server::entity as sepa_mandate_entity;
    use crate::shared::DEMO_TENANT_ID;

    #[async_trait]
    impl ServerCommand for DeleteSepaMandate {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            sepa_mandate_entity::Entity::delete_many()
                .filter(sepa_mandate_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(sepa_mandate_entity::Column::Id.eq(self.id.0.to_vec()))
                .exec(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE sepa_mandate id={}: {e}", self.id,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
