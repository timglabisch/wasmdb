use database::Database;
use rpc_command::rpc_command;
use sql_engine::execute::Params;
use sql_engine::storage::Uuid;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

use crate::command_helpers::{execute_sql, p_int, p_str, p_uuid};

#[rpc_command]
pub struct UpdateProduct {
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

impl Command for UpdateProduct {
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
            "UPDATE products SET sku = :sku, name = :name, description = :description, unit = :unit, unit_price = :unit_price, tax_rate = :tax_rate, cost_price = :cost_price, active = :active WHERE products.id = :id",
            params)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter, Set};
    use sync_server_mysql::ServerCommand;

    use crate::products::product_server::entity as product_entity;
    use crate::shared::DEMO_TENANT_ID;

    #[async_trait]
    impl ServerCommand for UpdateProduct {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let model = product_entity::Entity::find()
                .filter(product_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(product_entity::Column::Id.eq(self.id.0.to_vec()))
                .one(tx).await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "load product {}: {e}", self.id,
                )))?
                .ok_or_else(|| CommandError::ExecutionFailed(format!(
                    "product {} not found", self.id,
                )))?;

            let mut am: product_entity::ActiveModel = model.into();
            am.sku = Set(self.sku.clone());
            am.name = Set(self.name.clone());
            am.description = Set(self.description.clone());
            am.unit = Set(self.unit.clone());
            am.unit_price = Set(self.unit_price);
            am.tax_rate = Set(self.tax_rate);
            am.cost_price = Set(self.cost_price);
            am.active = Set(self.active);
            am.update(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                "UPDATE product {}: {e}", self.id,
            )))?;
            Ok(client_zset.clone())
        }
    }
}
