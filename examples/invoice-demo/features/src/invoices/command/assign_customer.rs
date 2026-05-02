use database::Database;
use rpc_command::rpc_command;
use sql_engine::storage::Uuid;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

use crate::command_helpers::execute_stmt;
use crate::shared::DEMO_TENANT_ID;

/// Intent-Command: assign (or remove) a customer from an invoice.
///
/// Replaces `updateInvoiceHeader({...customer_id}) + logActivity(...)`.
/// When the invoice's address fields are still empty the client wrapper
/// copies the customer's billing/shipping defaults into the payload so the
/// server never needs an extra customer lookup. The activity-log row is
/// produced inside this command — callers no longer compose it.
///
/// `customer_name` is empty when the customer is being removed; the detail
/// string switches from "Kunde \"…\" zugewiesen" to "Kunde entfernt".
#[rpc_command]
pub struct AssignCustomer {
    #[ts(type = "string")]
    pub id: Uuid,

    /// New customer (None = remove).
    #[ts(type = "string | null")]
    pub customer_id: Option<Uuid>,

    /// Customer display name used in the activity-log detail.
    /// Empty string when removing the customer.
    pub customer_name: String,

    // ── Address fields: final resolved values (may be copied from customer) ──
    pub billing_street: String,
    pub billing_zip: String,
    pub billing_city: String,
    pub billing_country: String,
    pub shipping_street: String,
    pub shipping_zip: String,
    pub shipping_city: String,
    pub shipping_country: String,

    /// Resolved due date (may be derived from customer payment_terms_days).
    pub date_due: String,

    /// Activity log fields.
    #[ts(type = "string")]
    #[client_default = "nextId()"]
    pub activity_id: Uuid,
    #[client_default = "new Date().toISOString()"]
    pub timestamp: String,
}

fn detail_for(customer_name: &str) -> String {
    if customer_name.is_empty() {
        "Kunde entfernt".to_string()
    } else {
        format!("Kunde \"{customer_name}\" zugewiesen")
    }
}

impl Command for AssignCustomer {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        let detail = detail_for(&self.customer_name);

        let mut acc = execute_stmt(
            db,
            sql!(
                "UPDATE invoices SET \
                 customer_id = {self.customer_id}, \
                 billing_street = {self.billing_street}, billing_zip = {self.billing_zip}, \
                 billing_city = {self.billing_city}, billing_country = {self.billing_country}, \
                 shipping_street = {self.shipping_street}, shipping_zip = {self.shipping_zip}, \
                 shipping_city = {self.shipping_city}, shipping_country = {self.shipping_country}, \
                 date_due = {self.date_due} \
                 WHERE invoices.id = {self.id}"
            ),
        )?;
        acc.extend(execute_stmt(
            db,
            sql!(
                "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
                 VALUES ({self.activity_id}, {self.timestamp}, 'invoice', {self.id}, 'customer_assigned', 'demo', {detail})"
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
    impl ServerCommand for AssignCustomer {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let detail = detail_for(&self.customer_name);

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

            let mut am: invoice_entity::ActiveModel = model.into();
            am.customer_id = Set(self.customer_id.as_ref().map(|u| u.0.to_vec()));
            am.billing_street = Set(self.billing_street.clone());
            am.billing_zip = Set(self.billing_zip.clone());
            am.billing_city = Set(self.billing_city.clone());
            am.billing_country = Set(self.billing_country.clone());
            am.shipping_street = Set(self.shipping_street.clone());
            am.shipping_zip = Set(self.shipping_zip.clone());
            am.shipping_city = Set(self.shipping_city.clone());
            am.shipping_country = Set(self.shipping_country.clone());
            am.date_due = Set(self.date_due.clone());
            am.update(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                "UPDATE invoice {} assign customer: {e}", self.id,
            )))?;

            insert_activity(
                tx,
                &self.activity_id,
                &self.timestamp,
                "invoice",
                &self.id,
                "customer_assigned",
                &detail,
            )
            .await?;

            Ok(client_zset.clone())
        }
    }
}
