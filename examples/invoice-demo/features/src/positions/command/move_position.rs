use sql_engine::storage::Uuid;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use rpc_command::rpc_command;
use crate::command_helpers::{execute_sql, p_int, p_uuid};
use crate::shared::DEMO_TENANT_ID;

#[rpc_command]
pub struct MovePosition {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "number")]
    pub new_position_nr: i64,
}

impl Command for MovePosition {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let params = Params::from([
            p_uuid("id", &self.id),
            p_int("position_nr", self.new_position_nr),
        ]);
        execute_sql(db,
            "UPDATE positions SET position_nr = :position_nr WHERE positions.id = :id",
            params)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter, Set};
    use sync_server_mysql::ServerCommand;

    use crate::positions::position_server::entity as position_entity;

    #[async_trait]
    impl ServerCommand for MovePosition {
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

            let mut am: position_entity::ActiveModel = model.into();
            am.position_nr = Set(self.new_position_nr);
            am.update(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                "UPDATE position {}: {e}", self.id,
            )))?;
            Ok(client_zset.clone())
        }
    }
}
