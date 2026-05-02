use sql_engine::storage::Uuid;
use database::Database;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use rpc_command::rpc_command;
use crate::command_helpers::SqlStmtExt;
use crate::shared::DEMO_TENANT_ID;

#[rpc_command]
pub struct UpdateRecurringPosition {
    #[ts(type = "string")]
    pub id: Uuid,
    pub description: String,
    #[ts(type = "number")]
    pub quantity: i64,
    #[ts(type = "number")]
    pub unit_price: i64,
    #[ts(type = "number")]
    pub tax_rate: i64,
    pub unit: String,
    pub item_number: String,
    #[ts(type = "number")]
    pub discount_pct: i64,
}

impl Command for UpdateRecurringPosition {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        sql!(
            "UPDATE recurring_positions SET description = {description}, quantity = {quantity}, unit_price = {unit_price}, tax_rate = {tax_rate}, unit = {unit}, item_number = {item_number}, discount_pct = {discount_pct} WHERE recurring_positions.id = {id}",
            id = self.id,
            description = self.description,
            quantity = self.quantity,
            unit_price = self.unit_price,
            tax_rate = self.tax_rate,
            unit = self.unit,
            item_number = self.item_number,
            discount_pct = self.discount_pct,
        )
        .execute(db)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter, Set};
    use sync_server_mysql::ServerCommand;

    use crate::recurring::recurring_position_server::entity as recurring_position_entity;

    #[async_trait]
    impl ServerCommand for UpdateRecurringPosition {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let model = recurring_position_entity::Entity::find()
                .filter(recurring_position_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(recurring_position_entity::Column::Id.eq(self.id.0.to_vec()))
                .one(tx).await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "load recurring_position {}: {e}", self.id,
                )))?
                .ok_or_else(|| CommandError::ExecutionFailed(format!(
                    "recurring_position {} not found", self.id,
                )))?;

            let mut am: recurring_position_entity::ActiveModel = model.into();
            am.description = Set(self.description.clone());
            am.quantity = Set(self.quantity);
            am.unit_price = Set(self.unit_price);
            am.tax_rate = Set(self.tax_rate);
            am.unit = Set(self.unit.clone());
            am.item_number = Set(self.item_number.clone());
            am.discount_pct = Set(self.discount_pct);
            am.update(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                "UPDATE recurring_position {}: {e}", self.id,
            )))?;
            Ok(client_zset.clone())
        }
    }
}
