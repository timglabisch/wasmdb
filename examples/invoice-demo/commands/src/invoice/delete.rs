use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use sql_engine::storage::Uuid;
use ts_rs::TS;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_str, p_uuid, DEMO_TENANT_ID};

/// Cascades positions + payments + invoice — all in one atomic ZSet.
/// Also writes an activity_log row (action='delete', entity_type='invoice').
/// `activity_id` + `timestamp` are supplied by the client so optimistic and
/// server-authoritative inserts share the same primary key (idempotent re-apply).
/// `number` is passed in because the invoice row is gone by the time a server
/// would try to read it back.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct DeleteInvoice {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "string")]
    pub activity_id: Uuid,
    pub timestamp: String,
    pub number: String,
}

fn detail_for(number: &str) -> String {
    format!("Beleg \"{number}\" gelöscht")
}

impl Command for DeleteInvoice {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let id = self.id;
        let detail = detail_for(&self.number);
        let mut acc = ZSet::new();
        let p = Params::from([p_uuid("iid", &id)]);
        acc.extend(execute_sql(db,
            "DELETE FROM payments WHERE invoice_id = :iid", p)?);
        let p = Params::from([p_uuid("iid", &id)]);
        acc.extend(execute_sql(db,
            "DELETE FROM positions WHERE invoice_id = :iid", p)?);
        let p = Params::from([p_uuid("id", &id)]);
        acc.extend(execute_sql(db,
            "DELETE FROM invoices WHERE id = :id", p)?);
        acc.extend(execute_sql(
            db,
            "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
             VALUES (:aid, :ts, 'invoice', :id, 'delete', 'demo', :detail)",
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
    impl ServerCommand for DeleteInvoice {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            sqlx::query("DELETE FROM payments WHERE tenant_id = ? AND invoice_id = ?")
                .bind(DEMO_TENANT_ID)
                .bind(&self.id.0[..])
                .execute(&mut **tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE payments for invoice {}: {e}",
                    self.id,
                )))?;
            sqlx::query("DELETE FROM positions WHERE tenant_id = ? AND invoice_id = ?")
                .bind(DEMO_TENANT_ID)
                .bind(&self.id.0[..])
                .execute(&mut **tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE positions for invoice {}: {e}",
                    self.id,
                )))?;
            sqlx::query("DELETE FROM invoices WHERE tenant_id = ? AND id = ?")
                .bind(DEMO_TENANT_ID)
                .bind(&self.id.0[..])
                .execute(&mut **tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE invoice {}: {e}",
                    self.id,
                )))?;

            let detail = detail_for(&self.number);
            sqlx::query(
                "INSERT INTO activity_log (tenant_id, id, timestamp, entity_type, entity_id, action, actor, detail) \
                 VALUES (?, ?, ?, 'invoice', ?, 'delete', 'demo', ?) \
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
