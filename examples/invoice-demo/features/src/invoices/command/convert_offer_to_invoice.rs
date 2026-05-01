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
    use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter, QuerySelect};
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
            invoice_entity::Entity::update_many()
                .col_expr(invoice_entity::Column::DocType, "invoice".to_string().into())
                .col_expr(invoice_entity::Column::Status, "draft".to_string().into())
                .filter(invoice_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(invoice_entity::Column::Id.eq(self.id.0.to_vec()))
                .exec(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "UPDATE invoice {} -> convert offer: {e}", self.id,
                )))?;

            let number: String = invoice_entity::Entity::find()
                .select_only()
                .column(invoice_entity::Column::Number)
                .filter(invoice_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(invoice_entity::Column::Id.eq(self.id.0.to_vec()))
                .into_tuple()
                .one(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "lookup number for invoice {}: {e}", self.id,
                )))?
                .ok_or_else(|| CommandError::ExecutionFailed(format!(
                    "invoice {} not found", self.id,
                )))?;
            let detail = detail_for(&number);

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
