use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct AddPosition {
    pub id: i64,
    pub invoice_id: i64,
    pub position_nr: i64,
    pub description: String,
    pub quantity: i64,
    pub unit_price: i64,
    pub tax_rate: i64,
    pub product_id: i64,
    pub item_number: String,
    pub unit: String,
    pub discount_pct: i64,
    pub cost_price: i64,
    pub position_type: String,
}

impl Command for AddPosition {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let params = Params::from([
            p_int("id", self.id),
            p_int("invoice_id", self.invoice_id),
            p_int("position_nr", self.position_nr),
            p_str("description", &self.description),
            p_int("quantity", self.quantity),
            p_int("unit_price", self.unit_price),
            p_int("tax_rate", self.tax_rate),
            p_int("product_id", self.product_id),
            p_str("item_number", &self.item_number),
            p_str("unit", &self.unit),
            p_int("discount_pct", self.discount_pct),
            p_int("cost_price", self.cost_price),
            p_str("position_type", &self.position_type),
        ]);
        execute_sql(db,
            "INSERT INTO positions (id, invoice_id, position_nr, description, quantity, unit_price, tax_rate, product_id, item_number, unit, discount_pct, cost_price, position_type) \
             VALUES (:id, :invoice_id, :position_nr, :description, :quantity, :unit_price, :tax_rate, :product_id, :item_number, :unit, :discount_pct, :cost_price, :position_type)",
            params)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use std::collections::HashMap;
    use async_trait::async_trait;
    use sql_engine::schema::TableSchema;
    use sqlx::{MySql, Transaction};
    use sync_server_mysql::{apply_zset, ServerCommand};

    #[async_trait]
    impl ServerCommand for AddPosition {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
            schemas: &HashMap<String, TableSchema>,
        ) -> Result<ZSet, CommandError> {
            apply_zset(tx, client_zset, schemas).await?;
            Ok(client_zset.clone())
        }
    }
}
