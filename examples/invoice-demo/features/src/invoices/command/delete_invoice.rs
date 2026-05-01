use sql_engine::storage::Uuid;
use database::Database;
use rpc_command::rpc_command;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::command_helpers::{execute_sql, p_str, p_uuid};
use crate::shared::DEMO_TENANT_ID;

/// Cascades positions + payments + invoice — all in one atomic ZSet.
/// Also writes an activity_log row (action='delete', entity_type='invoice').
/// `activity_id` + `timestamp` are supplied by the client so optimistic and
/// server-authoritative inserts share the same primary key (idempotent re-apply).
/// `number` is passed in because the invoice row is gone by the time a server
/// would try to read it back.
#[rpc_command]
pub struct DeleteInvoice {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "string")]
    #[client_default = "nextId()"]
    pub activity_id: Uuid,
    #[client_default = "new Date().toISOString()"]
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
    use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, ModelTrait, QueryFilter};
    use sync_server_mysql::ServerCommand;

    use crate::activity_log::activity_log_server::insert_activity;
    use crate::invoices::invoice_server::entity as invoice_entity;
    use crate::payments::payment_server::entity as payment_entity;
    use crate::positions::position_server::entity as position_entity;

    #[async_trait]
    impl ServerCommand for DeleteInvoice {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            payment_entity::Entity::delete_many()
                .filter(payment_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(payment_entity::Column::InvoiceId.eq(self.id.0.to_vec()))
                .exec(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE payments for invoice {}: {e}", self.id,
                )))?;

            position_entity::Entity::delete_many()
                .filter(position_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(position_entity::Column::InvoiceId.eq(self.id.0.to_vec()))
                .exec(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE positions for invoice {}: {e}", self.id,
                )))?;

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
            model.delete(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                "DELETE invoice {}: {e}", self.id,
            )))?;

            let detail = detail_for(&self.number);
            insert_activity(
                tx,
                &self.activity_id,
                &self.timestamp,
                "invoice",
                &self.id,
                "delete",
                &detail,
            )
            .await?;

            Ok(client_zset.clone())
        }
    }
}
