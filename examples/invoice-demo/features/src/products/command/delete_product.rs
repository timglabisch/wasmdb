use database::Database;
use rpc_command::rpc_command;
use sql_engine::execute::Params;
use sql_engine::storage::Uuid;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

use crate::command_helpers::{execute_sql, p_str, p_uuid};

#[rpc_command]
pub struct DeleteProduct {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "string")]
    #[client_default = "nextId()"]
    pub activity_id: Uuid,
    #[client_default = "new Date().toISOString()"]
    pub timestamp: String,
    pub name: String,
}

fn detail_for(name: &str) -> String {
    format!("Produkt \"{name}\" gelöscht")
}

impl Command for DeleteProduct {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let detail = detail_for(&self.name);
        let params = Params::from([p_uuid("id", &self.id)]);
        let mut acc = execute_sql(db, "DELETE FROM products WHERE products.id = :id", params)?;
        acc.extend(execute_sql(
            db,
            "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
             VALUES (:aid, :ts, 'product', :id, 'delete', 'demo', :detail)",
            Params::from([
                p_uuid("aid", &self.activity_id),
                p_str("ts", &self.timestamp),
                p_uuid("id", &self.id),
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
    use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter};
    use sync_server_mysql::ServerCommand;

    use crate::activity_log::activity_log_server::insert_activity;
    use crate::products::product_server::entity as product_entity;
    use crate::shared::DEMO_TENANT_ID;

    #[async_trait]
    impl ServerCommand for DeleteProduct {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            product_entity::Entity::delete_many()
                .filter(product_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(product_entity::Column::Id.eq(self.id.0.to_vec()))
                .exec(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE product id={}: {e}", self.id,
                )))?;

            let detail = detail_for(&self.name);
            insert_activity(
                tx,
                &self.activity_id,
                &self.timestamp,
                "product",
                &self.id,
                "delete",
                &detail,
            )
            .await?;

            Ok(client_zset.clone())
        }
    }
}
