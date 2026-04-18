use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use database::Database;
use sync::command::CommandError;
use sync::zset::ZSet;
use crate::helpers::execute_sql;
use super::params::invoice_params;

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct UpdateInvoiceHeader {
    pub id: i64,
    pub number: String,
    pub status: String,
    pub date_issued: String,
    pub date_due: String,
    pub notes: String,
    pub doc_type: String,
    pub parent_id: i64,
    pub service_date: String,
    pub cash_allowance_pct: i64,
    pub cash_allowance_days: i64,
    pub discount_pct: i64,
    pub payment_method: String,
    pub sepa_mandate_id: i64,
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

impl UpdateInvoiceHeader {
    pub fn execute(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        let params = invoice_params(
            self.id, None,
            &self.number, &self.status, &self.date_issued, &self.date_due, &self.notes,
            &self.doc_type, self.parent_id, &self.service_date,
            self.cash_allowance_pct, self.cash_allowance_days, self.discount_pct,
            &self.payment_method, self.sepa_mandate_id, &self.currency, &self.language,
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
