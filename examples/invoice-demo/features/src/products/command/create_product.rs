use database::Database;
use rpc_command::rpc_command;
use sql_engine::storage::Uuid;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

use crate::shared::DEMO_TENANT_ID;

#[rpc_command]
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
    #[ts(type = "string")]
    #[client_default = "nextId()"]
    pub activity_id: Uuid,
    #[client_default = "new Date().toISOString()"]
    pub timestamp: String,
}

fn detail_for(name: &str) -> String {
    format!("Produkt \"{name}\" angelegt")
}

impl Command for CreateProduct {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let detail = detail_for(&self.name);
        let mut acc = sql!(
            "INSERT INTO products (id, sku, name, description, unit, unit_price, tax_rate, cost_price, active) \
             VALUES ({self.id}, {self.sku}, {self.name}, {self.description}, {self.unit}, {self.unit_price}, {self.tax_rate}, {self.cost_price}, {self.active})"
        )
        .execute(db)?;
        acc.extend(
            sql!(
                "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
                 VALUES ({self.activity_id}, {self.timestamp}, 'product', {self.id}, 'create', 'demo', {detail})"
            )
            .execute(db)?,
        );
        Ok(acc)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sea_orm::{DatabaseTransaction, EntityTrait, Set};
    use sync_server_mysql::ServerCommand;

    use crate::activity_log::activity_log_server::insert_activity;
    use crate::products::product_server::entity as product_entity;

    #[async_trait]
    impl ServerCommand for CreateProduct {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let am = product_entity::ActiveModel {
                tenant_id: Set(DEMO_TENANT_ID),
                id: Set(self.id.0.to_vec()),
                sku: Set(self.sku.clone()),
                name: Set(self.name.clone()),
                description: Set(self.description.clone()),
                unit: Set(self.unit.clone()),
                unit_price: Set(self.unit_price),
                tax_rate: Set(self.tax_rate),
                cost_price: Set(self.cost_price),
                active: Set(self.active),
            };
            product_entity::Entity::insert(am)
                .on_conflict_do_nothing()
                .exec_without_returning(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "INSERT product id={}: {e}", self.id,
                )))?;

            let detail = detail_for(&self.name);
            insert_activity(
                tx,
                &self.activity_id,
                &self.timestamp,
                "product",
                &self.id,
                "create",
                &detail,
            )
            .await?;

            Ok(client_zset.clone())
        }
    }
}
