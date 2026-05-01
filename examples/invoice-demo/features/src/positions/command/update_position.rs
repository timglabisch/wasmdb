use sql_engine::storage::Uuid;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use rpc_command::rpc_command;
use crate::command_helpers::{execute_sql, p_int, p_str, p_uuid, p_uuid_opt};
use crate::shared::DEMO_TENANT_ID;

#[rpc_command]
pub struct UpdatePosition {
    #[ts(type = "string")]
    pub id: Uuid,
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

impl Command for UpdatePosition {
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
            p_uuid_opt("product_id", &self.product_id),
            p_str("item_number", &self.item_number),
            p_str("unit", &self.unit),
            p_int("discount_pct", self.discount_pct),
            p_int("cost_price", self.cost_price),
            p_str("position_type", &self.position_type),
        ]);
        execute_sql(db,
            "UPDATE positions SET description = :description, quantity = :quantity, \
             unit_price = :unit_price, tax_rate = :tax_rate, \
             product_id = :product_id, item_number = :item_number, unit = :unit, \
             discount_pct = :discount_pct, cost_price = :cost_price, position_type = :position_type \
             WHERE positions.id = :id",
            params)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sqlx::{MySql, Transaction};
    use sync_server_mysql::ServerCommand;

    #[async_trait]
    impl ServerCommand for UpdatePosition {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            sqlx::query(
                "UPDATE positions SET description = ?, quantity = ?, \
                 unit_price = ?, tax_rate = ?, \
                 product_id = ?, item_number = ?, unit = ?, \
                 discount_pct = ?, cost_price = ?, position_type = ? \
                 WHERE tenant_id = ? AND id = ?"
            )
                .bind(&self.description)
                .bind(self.quantity)
                .bind(self.unit_price)
                .bind(self.tax_rate)
                .bind(self.product_id.as_ref().map(|u| u.0.to_vec()))
                .bind(&self.item_number)
                .bind(&self.unit)
                .bind(self.discount_pct)
                .bind(self.cost_price)
                .bind(&self.position_type)
                .bind(DEMO_TENANT_ID)
                .bind(&self.id.0[..])
                .execute(&mut **tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "UPDATE position id={}: {e}",
                    self.id,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
