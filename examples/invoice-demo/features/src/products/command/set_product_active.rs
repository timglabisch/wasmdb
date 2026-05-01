use database::Database;
use rpc_command::rpc_command;
use sql_engine::execute::Params;
use sql_engine::storage::Uuid;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

use crate::command_helpers::{execute_sql, p_int, p_str, p_uuid, read_str_col};

/// Intent-Command: activate or deactivate a product. Replaces the old
/// `updateProduct({...,active}) + logActivity(...)` pair. Activity is
/// produced from the product name inside execute_optimistic / execute_server.
/// `activity_id` + `timestamp` are passed in by the client wrapper so client
/// and server inserts share the same primary key (idempotent re-apply).
#[rpc_command]
pub struct SetProductActive {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "number")]
    pub active: i64,
    #[ts(type = "string")]
    #[client_default = "nextId()"]
    pub activity_id: Uuid,
    #[client_default = "new Date().toISOString()"]
    pub timestamp: String,
}

fn detail_for(name: &str, active: i64) -> String {
    if active == 1 {
        format!("Produkt \"{name}\" aktiviert")
    } else {
        format!("Produkt \"{name}\" deaktiviert")
    }
}

fn action_for(active: i64) -> &'static str {
    if active == 1 { "activate" } else { "deactivate" }
}

impl Command for SetProductActive {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        let mut acc = execute_sql(
            db,
            "UPDATE products SET active = :active WHERE products.id = :id",
            Params::from([
                p_int("active", self.active),
                p_uuid("id", &self.id),
            ]),
        )?;

        let names = read_str_col(
            db,
            "SELECT products.name FROM products WHERE products.id = :id",
            Params::from([p_uuid("id", &self.id)]),
        )?;
        let name = names.into_iter().next().unwrap_or_default();
        let detail = detail_for(&name, self.active);
        let action = action_for(self.active);

        acc.extend(execute_sql(
            db,
            "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
             VALUES (:aid, :ts, 'product', :id, :action, 'demo', :detail)",
            Params::from([
                p_uuid("aid", &self.activity_id),
                p_str("ts", &self.timestamp),
                p_uuid("id", &self.id),
                p_str("action", action),
                p_str("detail", &detail),
            ]),
        )?);

        Ok(acc)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter, Set};
    use sync_server_mysql::ServerCommand;

    use crate::activity_log::activity_log_server::insert_activity;
    use crate::products::product_server::entity as product_entity;
    use crate::shared::DEMO_TENANT_ID;

    #[async_trait]
    impl ServerCommand for SetProductActive {
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

            let name = model.name.clone();

            let mut am: product_entity::ActiveModel = model.into();
            am.active = Set(self.active);
            am.update(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                "UPDATE product {}: {e}", self.id,
            )))?;

            let detail = detail_for(&name, self.active);
            let action = action_for(self.active);
            insert_activity(
                tx,
                &self.activity_id,
                &self.timestamp,
                "product",
                &self.id,
                action,
                &detail,
            )
            .await?;

            Ok(client_zset.clone())
        }
    }
}
