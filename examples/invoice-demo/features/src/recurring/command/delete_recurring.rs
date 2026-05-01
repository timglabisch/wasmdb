use sql_engine::storage::Uuid;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use rpc_command::rpc_command;
use crate::command_helpers::{execute_sql, p_str, p_uuid};
use crate::shared::DEMO_TENANT_ID;

/// Cascades recurring_positions + recurring_invoice atomically.
/// Also writes an activity_log row (action='delete', entity_type='recurring').
#[rpc_command]
pub struct DeleteRecurring {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "string")]
    #[client_default = "nextId()"]
    pub activity_id: Uuid,
    #[client_default = "new Date().toISOString()"]
    pub timestamp: String,
    pub label_for_detail: String,
}

fn detail_for(label: &str) -> String {
    format!("Serie \"{label}\" gelöscht")
}

impl Command for DeleteRecurring {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let id = self.id;
        let detail = detail_for(&self.label_for_detail);
        let mut acc = ZSet::new();
        let p = Params::from([p_uuid("rid", &id)]);
        acc.extend(execute_sql(db,
            "DELETE FROM recurring_positions WHERE recurring_id = :rid", p)?);
        let p = Params::from([p_uuid("id", &id)]);
        acc.extend(execute_sql(db,
            "DELETE FROM recurring_invoices WHERE id = :id", p)?);
        acc.extend(execute_sql(
            db,
            "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
             VALUES (:aid, :ts, 'recurring', :id, 'delete', 'demo', :detail)",
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
    use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, ModelTrait, QueryFilter};
    use sync_server_mysql::ServerCommand;

    use crate::activity_log::activity_log_server::insert_activity;
    use crate::recurring::recurring_invoice_server::entity as recurring_invoice_entity;
    use crate::recurring::recurring_position_server::entity as recurring_position_entity;

    #[async_trait]
    impl ServerCommand for DeleteRecurring {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let recurring_positions: Vec<recurring_position_entity::Model> = recurring_position_entity::Entity::find()
                .filter(recurring_position_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(recurring_position_entity::Column::RecurringId.eq(self.id.0.to_vec()))
                .all(tx).await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "load recurring_positions for recurring_id {}: {e}",
                    self.id,
                )))?;
            for rp in recurring_positions {
                rp.delete(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE recurring_position for recurring_id {}: {e}",
                    self.id,
                )))?;
            }
            let model = recurring_invoice_entity::Entity::find()
                .filter(recurring_invoice_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(recurring_invoice_entity::Column::Id.eq(self.id.0.to_vec()))
                .one(tx).await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "load recurring {}: {e}", self.id,
                )))?
                .ok_or_else(|| CommandError::ExecutionFailed(format!(
                    "recurring {} not found", self.id,
                )))?;
            model.delete(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                "DELETE recurring {}: {e}", self.id,
            )))?;

            let detail = detail_for(&self.label_for_detail);
            insert_activity(
                tx,
                &self.activity_id,
                &self.timestamp,
                "recurring",
                &self.id,
                "delete",
                &detail,
            )
            .await?;

            Ok(client_zset.clone())
        }
    }
}
