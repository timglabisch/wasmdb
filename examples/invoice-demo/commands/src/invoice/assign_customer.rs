use borsh::{BorshDeserialize, BorshSerialize};
use database::Database;
use serde::{Deserialize, Serialize};
use sql_engine::execute::Params;
use sql_engine::storage::Uuid;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use ts_rs::TS;

use crate::helpers::{execute_sql, p_str, p_uuid, p_uuid_opt, DEMO_TENANT_ID};

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
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
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
    pub activity_id: Uuid,
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

        let mut acc = execute_sql(
            db,
            "UPDATE invoices SET \
             customer_id = :customer_id, \
             billing_street = :billing_street, billing_zip = :billing_zip, \
             billing_city = :billing_city, billing_country = :billing_country, \
             shipping_street = :shipping_street, shipping_zip = :shipping_zip, \
             shipping_city = :shipping_city, shipping_country = :shipping_country, \
             date_due = :date_due \
             WHERE invoices.id = :id",
            Params::from([
                p_uuid_opt("customer_id", &self.customer_id),
                p_str("billing_street", &self.billing_street),
                p_str("billing_zip", &self.billing_zip),
                p_str("billing_city", &self.billing_city),
                p_str("billing_country", &self.billing_country),
                p_str("shipping_street", &self.shipping_street),
                p_str("shipping_zip", &self.shipping_zip),
                p_str("shipping_city", &self.shipping_city),
                p_str("shipping_country", &self.shipping_country),
                p_str("date_due", &self.date_due),
                p_uuid("id", &self.id),
            ]),
        )?;

        acc.extend(execute_sql(
            db,
            "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
             VALUES (:aid, :ts, 'invoice', :id, 'customer_assigned', 'demo', :detail)",
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
    impl ServerCommand for AssignCustomer {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let detail = detail_for(&self.customer_name);

            sqlx::query(
                "UPDATE invoices SET \
                 customer_id = ?, \
                 billing_street = ?, billing_zip = ?, \
                 billing_city = ?, billing_country = ?, \
                 shipping_street = ?, shipping_zip = ?, \
                 shipping_city = ?, shipping_country = ?, \
                 date_due = ? \
                 WHERE invoices.tenant_id = ? AND invoices.id = ?",
            )
            .bind(self.customer_id.as_ref().map(|u| u.0.to_vec()))
            .bind(&self.billing_street)
            .bind(&self.billing_zip)
            .bind(&self.billing_city)
            .bind(&self.billing_country)
            .bind(&self.shipping_street)
            .bind(&self.shipping_zip)
            .bind(&self.shipping_city)
            .bind(&self.shipping_country)
            .bind(&self.date_due)
            .bind(DEMO_TENANT_ID)
            .bind(&self.id.0[..])
            .execute(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "UPDATE invoice {} assign customer: {e}", self.id,
            )))?;

            sqlx::query(
                "INSERT INTO activity_log \
                 (tenant_id, id, timestamp, entity_type, entity_id, action, actor, detail) \
                 VALUES (?, ?, ?, 'invoice', ?, 'customer_assigned', 'demo', ?) \
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
