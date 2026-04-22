use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct AddRecurringPosition {
    pub id: i64,
    pub recurring_id: i64,
    pub position_nr: i64,
    pub description: String,
    pub quantity: i64,
    pub unit_price: i64,
    pub tax_rate: i64,
    pub unit: String,
    pub item_number: String,
    pub discount_pct: i64,
}

impl Command for AddRecurringPosition {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let params = Params::from([
            p_int("id", self.id),
            p_int("recurring_id", self.recurring_id),
            p_int("position_nr", self.position_nr),
            p_str("description", &self.description),
            p_int("quantity", self.quantity),
            p_int("unit_price", self.unit_price),
            p_int("tax_rate", self.tax_rate),
            p_str("unit", &self.unit),
            p_str("item_number", &self.item_number),
            p_int("discount_pct", self.discount_pct),
        ]);
        execute_sql(db,
            "INSERT INTO recurring_positions (id, recurring_id, position_nr, description, quantity, unit_price, tax_rate, unit, item_number, discount_pct) \
             VALUES (:id, :recurring_id, :position_nr, :description, :quantity, :unit_price, :tax_rate, :unit, :item_number, :discount_pct)",
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
    impl ServerCommand for AddRecurringPosition {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            sqlx::query(
                "INSERT INTO recurring_positions (id, recurring_id, position_nr, description, quantity, unit_price, tax_rate, unit, item_number, discount_pct) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
                .bind(self.id)
                .bind(self.recurring_id)
                .bind(self.position_nr)
                .bind(&self.description)
                .bind(self.quantity)
                .bind(self.unit_price)
                .bind(self.tax_rate)
                .bind(&self.unit)
                .bind(&self.item_number)
                .bind(self.discount_pct)
                .execute(&mut **tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "INSERT recurring_position {}: {e}",
                    self.id,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
