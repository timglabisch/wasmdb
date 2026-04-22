use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct UpdateRecurringPosition {
    pub id: i64,
    pub description: String,
    pub quantity: i64,
    pub unit_price: i64,
    pub tax_rate: i64,
    pub unit: String,
    pub item_number: String,
    pub discount_pct: i64,
}

impl Command for UpdateRecurringPosition {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let params = Params::from([
            p_int("id", self.id),
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
    use std::collections::HashMap;
    use async_trait::async_trait;
    use sql_engine::schema::TableSchema;
    use sqlx::{MySql, Transaction};
    use sync_server_mysql::{apply_zset, ServerCommand};

    #[async_trait]
    impl ServerCommand for UpdateRecurringPosition {
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
