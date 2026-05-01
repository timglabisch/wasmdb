use sql_engine::storage::Uuid;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use rpc_command::rpc_command;
use crate::command_helpers::{execute_sql, p_int, p_str, p_uuid};
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
        let params = Params::from([
            p_uuid("id", &self.id),
            p_str("description", &self.description),
            p_int("quantity", self.quantity),
            p_int("unit_price", self.unit_price),
            p_int("tax_rate", self.tax_rate),
            p_str("unit", &self.unit),
            p_str("item_number", &self.item_number),
            p_int("discount_pct", self.discount_pct),
        ]);
        execute_sql(db,
            "UPDATE recurring_positions SET description = :description, quantity = :quantity, unit_price = :unit_price, tax_rate = :tax_rate, unit = :unit, item_number = :item_number, discount_pct = :discount_pct WHERE recurring_positions.id = :id",
            params)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter};
    use sync_server_mysql::ServerCommand;

    use crate::recurring::recurring_position_server::entity as recurring_position_entity;

    #[async_trait]
    impl ServerCommand for UpdateRecurringPosition {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            recurring_position_entity::Entity::update_many()
                .col_expr(recurring_position_entity::Column::Description, self.description.clone().into())
                .col_expr(recurring_position_entity::Column::Quantity, self.quantity.into())
                .col_expr(recurring_position_entity::Column::UnitPrice, self.unit_price.into())
                .col_expr(recurring_position_entity::Column::TaxRate, self.tax_rate.into())
                .col_expr(recurring_position_entity::Column::Unit, self.unit.clone().into())
                .col_expr(recurring_position_entity::Column::ItemNumber, self.item_number.clone().into())
                .col_expr(recurring_position_entity::Column::DiscountPct, self.discount_pct.into())
                .filter(recurring_position_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(recurring_position_entity::Column::Id.eq(self.id.0.to_vec()))
                .exec(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "UPDATE recurring_position {}: {e}",
                    self.id,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
