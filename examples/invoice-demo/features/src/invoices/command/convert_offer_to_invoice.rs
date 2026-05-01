use database::Database;
use rpc_command::rpc_command;
use sql_engine::execute::Params;
use sql_engine::storage::Uuid;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

use crate::command_helpers::{execute_sql, p_str, p_uuid, read_str_col};
use crate::shared::DEMO_TENANT_ID;

/// Intent-Command: convert an offer document into a draft invoice.
///
/// Sets `doc_type = 'invoice'` and `status = 'draft'` on the document and
/// appends an `offer_converted` activity-log row. `activity_id` + `timestamp`
/// are supplied by the client wrapper so the two impls share the same PK
/// (idempotent re-apply).
#[rpc_command]
pub struct ConvertOfferToInvoice {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "string")]
    #[client_default = "nextId()"]
    pub activity_id: Uuid,
    #[client_default = "new Date().toISOString()"]
    pub timestamp: String,
}

fn detail_for(number: &str) -> String {
    format!("Angebot \"{number}\" in Rechnung umgewandelt")
}

impl Command for ConvertOfferToInvoice {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        let numbers = read_str_col(
            db,
            "SELECT invoices.number FROM invoices WHERE invoices.id = :id",
            Params::from([p_uuid("id", &self.id)]),
        )?;
        let number = numbers.into_iter().next().unwrap_or_default();
        let detail = detail_for(&number);

        let mut acc = execute_sql(
            db,
            "UPDATE invoices SET doc_type = 'invoice', status = 'draft' \
             WHERE invoices.id = :id",
            Params::from([p_uuid("id", &self.id)]),
        )?;

        acc.extend(execute_sql(
            db,
            "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
             VALUES (:aid, :ts, 'invoice', :id, 'offer_converted', 'demo', :detail)",
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
    impl ServerCommand for ConvertOfferToInvoice {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            sqlx::query(
                "UPDATE invoices SET doc_type = 'invoice', status = 'draft' \
                 WHERE invoices.tenant_id = ? AND invoices.id = ?",
            )
            .bind(DEMO_TENANT_ID)
            .bind(&self.id.0[..])
            .execute(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "UPDATE invoice {} -> convert offer: {e}", self.id,
            )))?;

            let number: String = sqlx::query_scalar(
                "SELECT number FROM invoices WHERE tenant_id = ? AND id = ?",
            )
            .bind(DEMO_TENANT_ID)
            .bind(&self.id.0[..])
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "lookup number for invoice {}: {e}", self.id,
            )))?;
            let detail = detail_for(&number);

            sqlx::query(
                "INSERT INTO activity_log (tenant_id, id, timestamp, entity_type, entity_id, action, actor, detail) \
                 VALUES (?, ?, ?, 'invoice', ?, 'offer_converted', 'demo', ?) \
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
