use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct CreateProduct {
    pub id: i64,
    pub sku: String,
    pub name: String,
    pub description: String,
    pub unit: String,
    pub unit_price: i64,
    pub tax_rate: i64,
    pub cost_price: i64,
    pub active: i64,
}

impl Command for CreateProduct {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let params = Params::from([
            p_int("id", self.id),
            p_str("sku", &self.sku),
            p_str("name", &self.name),
            p_str("description", &self.description),
            p_str("unit", &self.unit),
            p_int("unit_price", self.unit_price),
            p_int("tax_rate", self.tax_rate),
            p_int("cost_price", self.cost_price),
            p_int("active", self.active),
        ]);
        execute_sql(db,
            "INSERT INTO products (id, sku, name, description, unit, unit_price, tax_rate, cost_price, active) \
             VALUES (:id, :sku, :name, :description, :unit, :unit_price, :tax_rate, :cost_price, :active)",
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
    impl ServerCommand for CreateProduct {
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
