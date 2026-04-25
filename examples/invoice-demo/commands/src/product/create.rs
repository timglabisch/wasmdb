use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use sql_engine::storage::Uuid;
use ts_rs::TS;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str, p_uuid, DEMO_TENANT_ID};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct CreateProduct {
    #[ts(type = "string")]
    pub id: Uuid,
    pub sku: String,
    pub name: String,
    pub description: String,
    pub unit: String,
    #[ts(type = "number")]
    pub unit_price: i64,
    #[ts(type = "number")]
    pub tax_rate: i64,
    #[ts(type = "number")]
    pub cost_price: i64,
    #[ts(type = "number")]
    pub active: i64,
}

impl Command for CreateProduct {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let params = Params::from([
            p_uuid("id", &self.id),
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
    use async_trait::async_trait;
    use sqlx::{MySql, Transaction};
    use sync_server_mysql::ServerCommand;

    #[async_trait]
    impl ServerCommand for CreateProduct {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            sqlx::query(
                "INSERT INTO products (tenant_id, id, sku, name, description, unit, unit_price, tax_rate, cost_price, active) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .bind(DEMO_TENANT_ID)
                .bind(&self.id.0[..])
                .bind(&self.sku)
                .bind(&self.name)
                .bind(&self.description)
                .bind(&self.unit)
                .bind(self.unit_price)
                .bind(self.tax_rate)
                .bind(self.cost_price)
                .bind(self.active)
                .execute(&mut **tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "INSERT product id={}: {e}",
                    self.id,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
