use database::Database;
use rpc_command::rpc_command;
use sql_engine::storage::Uuid;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

use crate::shared::DEMO_TENANT_ID;

/// Intent-Command: mark an invoice as paid.
///
/// Replaces the old `updateInvoiceHeader({...,status:'paid'}) + logActivity(...)`
/// pair. The activity row is produced inside `execute_optimistic` /
/// `execute_server` from the invoice number — callers no longer compose
/// the audit-log entry themselves. `activity_id` + `timestamp` are auto-filled
/// client-side via `#[client_default]` so client-optimistic and server-
/// authoritative inserts share the same primary key (idempotent re-apply).
#[rpc_command]
pub struct MarkPaid {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "string")]
    #[client_default = "nextId()"]
    pub activity_id: Uuid,
    #[client_default = "new Date().toISOString()"]
    pub timestamp: String,
}

fn detail_for(number: &str) -> String {
    format!("\"{number}\" als bezahlt markiert")
}

impl Command for MarkPaid {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        let numbers = sql!(
            "SELECT invoices.number FROM invoices WHERE invoices.id = {self.id}"
        )
        .read_str_col(db)?;
        let number = numbers.into_iter().next().unwrap_or_default();
        let detail = detail_for(&number);

        let mut acc = sql!("UPDATE invoices SET status = 'paid' WHERE invoices.id = {self.id}")
            .execute(db)?;
        acc.extend(
            sql!(
                "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
                 VALUES ({self.activity_id}, {self.timestamp}, 'invoice', {self.id}, 'status_paid', 'demo', {detail})"
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
    use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter, Set};
    use sync_server_mysql::ServerCommand;

    use crate::activity_log::activity_log_server::insert_activity;
    use crate::invoices::invoice_server::entity as invoice_entity;

    #[async_trait]
    impl ServerCommand for MarkPaid {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let model = invoice_entity::Entity::find()
                .filter(invoice_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(invoice_entity::Column::Id.eq(self.id.0.to_vec()))
                .one(tx).await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "load invoice {}: {e}", self.id,
                )))?
                .ok_or_else(|| CommandError::ExecutionFailed(format!(
                    "invoice {} not found", self.id,
                )))?;
            let number = model.number.clone();
            let mut am: invoice_entity::ActiveModel = model.into();
            am.status = Set("paid".to_string());
            am.update(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                "UPDATE invoice {} -> paid: {e}", self.id,
            )))?;
            let detail = detail_for(&number);

            insert_activity(
                tx,
                &self.activity_id,
                &self.timestamp,
                "invoice",
                &self.id,
                "status_paid",
                &detail,
            )
            .await?;

            Ok(client_zset.clone())
        }
    }
}
