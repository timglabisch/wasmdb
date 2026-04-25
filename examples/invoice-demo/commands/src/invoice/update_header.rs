use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use sql_engine::storage::Uuid;
use ts_rs::TS;
use database::Database;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::helpers::{execute_sql, DEMO_TENANT_ID};
use super::params::invoice_params;

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct UpdateInvoiceHeader {
    #[ts(type = "string")]
    pub id: Uuid,
    pub number: String,
    pub status: String,
    pub date_issued: String,
    pub date_due: String,
    pub notes: String,
    pub doc_type: String,
    #[ts(type = "string | null")]
    pub parent_id: Option<Uuid>,
    pub service_date: String,
    #[ts(type = "number")]
    pub cash_allowance_pct: i64,
    #[ts(type = "number")]
    pub cash_allowance_days: i64,
    #[ts(type = "number")]
    pub discount_pct: i64,
    pub payment_method: String,
    #[ts(type = "string | null")]
    pub sepa_mandate_id: Option<Uuid>,
    pub currency: String,
    pub language: String,
    pub project_ref: String,
    pub external_id: String,
    pub billing_street: String,
    pub billing_zip: String,
    pub billing_city: String,
    pub billing_country: String,
    pub shipping_street: String,
    pub shipping_zip: String,
    pub shipping_city: String,
    pub shipping_country: String,
}

impl Command for UpdateInvoiceHeader {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let params = invoice_params(
            &self.id, None,
            &self.number, &self.status, &self.date_issued, &self.date_due, &self.notes,
            &self.doc_type, &self.parent_id, &self.service_date,
            self.cash_allowance_pct, self.cash_allowance_days, self.discount_pct,
            &self.payment_method, &self.sepa_mandate_id, &self.currency, &self.language,
            &self.project_ref, &self.external_id,
            &self.billing_street, &self.billing_zip, &self.billing_city, &self.billing_country,
            &self.shipping_street, &self.shipping_zip, &self.shipping_city, &self.shipping_country,
        );
        execute_sql(db,
            "UPDATE invoices SET number = :number, status = :status, \
             date_issued = :date_issued, date_due = :date_due, notes = :notes, \
             doc_type = :doc_type, parent_id = :parent_id, service_date = :service_date, \
             cash_allowance_pct = :cash_allowance_pct, cash_allowance_days = :cash_allowance_days, discount_pct = :discount_pct, \
             payment_method = :payment_method, sepa_mandate_id = :sepa_mandate_id, \
             currency = :currency, language = :language, \
             project_ref = :project_ref, external_id = :external_id, \
             billing_street = :billing_street, billing_zip = :billing_zip, billing_city = :billing_city, billing_country = :billing_country, \
             shipping_street = :shipping_street, shipping_zip = :shipping_zip, shipping_city = :shipping_city, shipping_country = :shipping_country \
             WHERE invoices.id = :id",
            params)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sqlx::{MySql, Transaction};
    use sync_server_mysql::ServerCommand;

    #[async_trait]
    impl ServerCommand for UpdateInvoiceHeader {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            sqlx::query(
                "UPDATE invoices SET number = ?, status = ?, \
                 date_issued = ?, date_due = ?, notes = ?, \
                 doc_type = ?, parent_id = ?, service_date = ?, \
                 cash_allowance_pct = ?, cash_allowance_days = ?, discount_pct = ?, \
                 payment_method = ?, sepa_mandate_id = ?, \
                 currency = ?, language = ?, \
                 project_ref = ?, external_id = ?, \
                 billing_street = ?, billing_zip = ?, billing_city = ?, billing_country = ?, \
                 shipping_street = ?, shipping_zip = ?, shipping_city = ?, shipping_country = ? \
                 WHERE invoices.tenant_id = ? AND invoices.id = ?",
            )
                .bind(&self.number)
                .bind(&self.status)
                .bind(&self.date_issued)
                .bind(&self.date_due)
                .bind(&self.notes)
                .bind(&self.doc_type)
                .bind(self.parent_id.as_ref().map(|u| u.0.to_vec()))
                .bind(&self.service_date)
                .bind(self.cash_allowance_pct)
                .bind(self.cash_allowance_days)
                .bind(self.discount_pct)
                .bind(&self.payment_method)
                .bind(self.sepa_mandate_id.as_ref().map(|u| u.0.to_vec()))
                .bind(&self.currency)
                .bind(&self.language)
                .bind(&self.project_ref)
                .bind(&self.external_id)
                .bind(&self.billing_street)
                .bind(&self.billing_zip)
                .bind(&self.billing_city)
                .bind(&self.billing_country)
                .bind(&self.shipping_street)
                .bind(&self.shipping_zip)
                .bind(&self.shipping_city)
                .bind(&self.shipping_country)
                .bind(DEMO_TENANT_ID)
                .bind(&self.id.0[..])
                .execute(&mut **tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "UPDATE invoice {}: {e}",
                    self.id,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
