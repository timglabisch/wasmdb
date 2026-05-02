use sql_engine::storage::Uuid;
use database::Database;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use rpc_command::rpc_command;
use crate::command_helpers::SqlStmtExt;
use crate::shared::DEMO_TENANT_ID;

#[rpc_command]
pub struct AddRecurringPosition {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "string")]
    pub recurring_id: Uuid,
    #[ts(type = "number")]
    pub position_nr: i64,
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

impl Command for AddRecurringPosition {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let Self {
            id, recurring_id, position_nr, description, quantity, unit_price, tax_rate,
            unit, item_number, discount_pct,
        } = self;
        sql!(
            "INSERT INTO recurring_positions (id, recurring_id, position_nr, description, quantity, unit_price, tax_rate, unit, item_number, discount_pct) \
             VALUES ({id}, {recurring_id}, {position_nr}, {description}, {quantity}, {unit_price}, {tax_rate}, {unit}, {item_number}, {discount_pct})"
        )
        .execute(db)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sea_orm::{DatabaseTransaction, EntityTrait, Set};
    use sync_server_mysql::ServerCommand;

    use crate::recurring::recurring_position_server::entity as recurring_position_entity;

    #[async_trait]
    impl ServerCommand for AddRecurringPosition {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let am = recurring_position_entity::ActiveModel {
                tenant_id: Set(DEMO_TENANT_ID),
                id: Set(self.id.0.to_vec()),
                recurring_id: Set(self.recurring_id.0.to_vec()),
                position_nr: Set(self.position_nr),
                description: Set(self.description.clone()),
                quantity: Set(self.quantity),
                unit_price: Set(self.unit_price),
                tax_rate: Set(self.tax_rate),
                unit: Set(self.unit.clone()),
                item_number: Set(self.item_number.clone()),
                discount_pct: Set(self.discount_pct),
            };
            recurring_position_entity::Entity::insert(am)
                .on_conflict_do_nothing()
                .exec_without_returning(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "INSERT recurring_position {}: {e}",
                    self.id,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
