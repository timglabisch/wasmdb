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
    use sqlx::{MySql, Transaction};
    use sync_server_mysql::ServerCommand;

    #[async_trait]
    impl ServerCommand for DeleteRecurring {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            sqlx::query("DELETE FROM recurring_positions WHERE tenant_id = ? AND recurring_id = ?")
                .bind(DEMO_TENANT_ID)
                .bind(&self.id.0[..])
                .execute(&mut **tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE recurring_positions for recurring_id {}: {e}",
                    self.id,
                )))?;
            sqlx::query("DELETE FROM recurring_invoices WHERE tenant_id = ? AND id = ?")
                .bind(DEMO_TENANT_ID)
                .bind(&self.id.0[..])
                .execute(&mut **tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE recurring_invoice {}: {e}",
                    self.id,
                )))?;

            let detail = detail_for(&self.label_for_detail);
            sqlx::query(
                "INSERT INTO activity_log (tenant_id, id, timestamp, entity_type, entity_id, action, actor, detail) \
                 VALUES (?, ?, ?, 'recurring', ?, 'delete', 'demo', ?) \
                 ON DUPLICATE KEY UPDATE id = id",
            )
            .bind(DEMO_TENANT_ID)
            .bind(&self.activity_id.0[..])
            .bind(&self.timestamp)
            .bind(&self.id.0[..])
            .bind(&detail)
            .execute(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "INSERT activity {}: {e}", self.activity_id,
            )))?;

            Ok(client_zset.clone())
        }
    }
}
