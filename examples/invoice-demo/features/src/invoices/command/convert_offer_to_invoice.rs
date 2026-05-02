use database::Database;
use rpc_command::rpc_command;
use sql_engine::execute::Params;
use sql_engine::storage::Uuid;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

use crate::command_helpers::{execute_stmt, p_uuid, read_str_col};
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

        let mut acc = execute_stmt(
            db,
            sql!(
                "UPDATE invoices SET doc_type = 'invoice', status = 'draft' \
                 WHERE invoices.id = {self.id}"
            ),
        )?;
        acc.extend(execute_stmt(
            db,
            sql!(
                "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
                 VALUES ({self.activity_id}, {self.timestamp}, 'invoice', {self.id}, 'offer_converted', 'demo', {detail})"
            ),
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
    use crate::invoices::invoice_server::entity as invoice_entity;

    #[async_trait]
    impl ServerCommand for ConvertOfferToInvoice {
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
            let detail = detail_for(&number);

            let mut am: invoice_entity::ActiveModel = model.into();
            am.doc_type = Set("invoice".to_string());
            am.status = Set("draft".to_string());
            am.update(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                "UPDATE invoice {} -> convert offer: {e}", self.id,
            )))?;

            insert_activity(
                tx,
                &self.activity_id,
                &self.timestamp,
                "invoice",
                &self.id,
                "offer_converted",
                &detail,
            )
            .await?;

            Ok(client_zset.clone())
        }
    }
}
