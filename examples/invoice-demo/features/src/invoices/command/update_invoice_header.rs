use sql_engine::storage::Uuid;
use database::Database;
use rpc_command::rpc_command;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::command_helpers::execute_sql;
use crate::shared::DEMO_TENANT_ID;
use super::invoice_params::invoice_params;

#[rpc_command]
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
    use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter};
    use sync_server_mysql::ServerCommand;

    use crate::invoices::invoice_server::entity as invoice_entity;

    #[async_trait]
    impl ServerCommand for UpdateInvoiceHeader {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            invoice_entity::Entity::update_many()
                .col_expr(invoice_entity::Column::Number, self.number.clone().into())
                .col_expr(invoice_entity::Column::Status, self.status.clone().into())
                .col_expr(invoice_entity::Column::DateIssued, self.date_issued.clone().into())
                .col_expr(invoice_entity::Column::DateDue, self.date_due.clone().into())
                .col_expr(invoice_entity::Column::Notes, self.notes.clone().into())
                .col_expr(invoice_entity::Column::DocType, self.doc_type.clone().into())
                .col_expr(invoice_entity::Column::ParentId, self.parent_id.as_ref().map(|u| u.0.to_vec()).into())
                .col_expr(invoice_entity::Column::ServiceDate, self.service_date.clone().into())
                .col_expr(invoice_entity::Column::CashAllowancePct, self.cash_allowance_pct.into())
                .col_expr(invoice_entity::Column::CashAllowanceDays, self.cash_allowance_days.into())
                .col_expr(invoice_entity::Column::DiscountPct, self.discount_pct.into())
                .col_expr(invoice_entity::Column::PaymentMethod, self.payment_method.clone().into())
                .col_expr(invoice_entity::Column::SepaMandateId, self.sepa_mandate_id.as_ref().map(|u| u.0.to_vec()).into())
                .col_expr(invoice_entity::Column::Currency, self.currency.clone().into())
                .col_expr(invoice_entity::Column::Language, self.language.clone().into())
                .col_expr(invoice_entity::Column::ProjectRef, self.project_ref.clone().into())
                .col_expr(invoice_entity::Column::ExternalId, self.external_id.clone().into())
                .col_expr(invoice_entity::Column::BillingStreet, self.billing_street.clone().into())
                .col_expr(invoice_entity::Column::BillingZip, self.billing_zip.clone().into())
                .col_expr(invoice_entity::Column::BillingCity, self.billing_city.clone().into())
                .col_expr(invoice_entity::Column::BillingCountry, self.billing_country.clone().into())
                .col_expr(invoice_entity::Column::ShippingStreet, self.shipping_street.clone().into())
                .col_expr(invoice_entity::Column::ShippingZip, self.shipping_zip.clone().into())
                .col_expr(invoice_entity::Column::ShippingCity, self.shipping_city.clone().into())
                .col_expr(invoice_entity::Column::ShippingCountry, self.shipping_country.clone().into())
                .filter(invoice_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(invoice_entity::Column::Id.eq(self.id.0.to_vec()))
                .exec(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "UPDATE invoice {}: {e}", self.id,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
