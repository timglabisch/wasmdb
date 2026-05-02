use sql_engine::storage::Uuid;
use database::Database;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use rpc_command::rpc_command;
use crate::command_helpers::execute_stmt;
use crate::shared::DEMO_TENANT_ID;

#[rpc_command]
pub struct AddPosition {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "string")]
    pub invoice_id: Uuid,
    #[ts(type = "number")]
    pub position_nr: i64,
    pub description: String,
    #[ts(type = "number")]
    pub quantity: i64,
    #[ts(type = "number")]
    pub unit_price: i64,
    #[ts(type = "number")]
    pub tax_rate: i64,
    #[ts(type = "string | null")]
    pub product_id: Option<Uuid>,
    pub item_number: String,
    pub unit: String,
    #[ts(type = "number")]
    pub discount_pct: i64,
    #[ts(type = "number")]
    pub cost_price: i64,
    pub position_type: String,
}

impl Command for AddPosition {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let Self {
            id, invoice_id, position_nr, description, quantity, unit_price, tax_rate,
            product_id, item_number, unit, discount_pct, cost_price, position_type,
        } = self;
        execute_stmt(
            db,
            sql!(
                "INSERT INTO positions (id, invoice_id, position_nr, description, quantity, unit_price, tax_rate, product_id, item_number, unit, discount_pct, cost_price, position_type) \
                 VALUES ({id}, {invoice_id}, {position_nr}, {description}, {quantity}, {unit_price}, {tax_rate}, {product_id}, {item_number}, {unit}, {discount_pct}, {cost_price}, {position_type})"
            ),
        )
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sea_orm::{DatabaseTransaction, EntityTrait, Set};
    use sync_server_mysql::ServerCommand;

    use crate::positions::position_server::entity as position_entity;

    #[async_trait]
    impl ServerCommand for AddPosition {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let am = position_entity::ActiveModel {
                tenant_id: Set(DEMO_TENANT_ID),
                id: Set(self.id.0.to_vec()),
                invoice_id: Set(self.invoice_id.0.to_vec()),
                position_nr: Set(self.position_nr),
                description: Set(self.description.clone()),
                quantity: Set(self.quantity),
                unit_price: Set(self.unit_price),
                tax_rate: Set(self.tax_rate),
                product_id: Set(self.product_id.as_ref().map(|u| u.0.to_vec())),
                item_number: Set(self.item_number.clone()),
                unit: Set(self.unit.clone()),
                discount_pct: Set(self.discount_pct),
                cost_price: Set(self.cost_price),
                position_type: Set(self.position_type.clone()),
            };
            position_entity::Entity::insert(am)
                .on_conflict_do_nothing()
                .exec_without_returning(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "INSERT position id={} position_nr={}: {e}",
                    self.id, self.position_nr,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
