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
    use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter};
    use sync_server_mysql::ServerCommand;

    use crate::positions::position_server::entity as position_entity;

    #[async_trait]
    impl ServerCommand for MovePosition {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            position_entity::Entity::update_many()
                .col_expr(position_entity::Column::PositionNr, self.new_position_nr.into())
                .filter(position_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(position_entity::Column::Id.eq(self.id.0.to_vec()))
                .exec(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "UPDATE position id={} position_nr={}: {e}",
                    self.id, self.new_position_nr,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
