use sql_engine::storage::Uuid;
use database::Database;
use rpc_command::rpc_command;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::command_helpers::SqlStmtExt;
use crate::shared::DEMO_TENANT_ID;

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
        sql!(
            "UPDATE invoices SET number = {number}, status = {status}, \
             date_issued = {date_issued}, date_due = {date_due}, notes = {notes}, \
             doc_type = {doc_type}, parent_id = {parent_id}, service_date = {service_date}, \
             cash_allowance_pct = {cash_allowance_pct}, cash_allowance_days = {cash_allowance_days}, discount_pct = {discount_pct}, \
             payment_method = {payment_method}, sepa_mandate_id = {sepa_mandate_id}, \
             currency = {currency}, language = {language}, \
             project_ref = {project_ref}, external_id = {external_id}, \
             billing_street = {billing_street}, billing_zip = {billing_zip}, billing_city = {billing_city}, billing_country = {billing_country}, \
             shipping_street = {shipping_street}, shipping_zip = {shipping_zip}, shipping_city = {shipping_city}, shipping_country = {shipping_country} \
             WHERE invoices.id = {id}",
            id = self.id,
            number = self.number,
            status = self.status,
            date_issued = self.date_issued,
            date_due = self.date_due,
            notes = self.notes,
            doc_type = self.doc_type,
            parent_id = self.parent_id,
            service_date = self.service_date,
            cash_allowance_pct = self.cash_allowance_pct,
            cash_allowance_days = self.cash_allowance_days,
            discount_pct = self.discount_pct,
            payment_method = self.payment_method,
            sepa_mandate_id = self.sepa_mandate_id,
            currency = self.currency,
            language = self.language,
            project_ref = self.project_ref,
            external_id = self.external_id,
            billing_street = self.billing_street,
            billing_zip = self.billing_zip,
            billing_city = self.billing_city,
            billing_country = self.billing_country,
            shipping_street = self.shipping_street,
            shipping_zip = self.shipping_zip,
            shipping_city = self.shipping_city,
            shipping_country = self.shipping_country,
        )
        .execute(db)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter, Set};
    use sync_server_mysql::ServerCommand;

    use crate::invoices::invoice_server::entity as invoice_entity;

    #[async_trait]
    impl ServerCommand for UpdateInvoiceHeader {
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

            let mut am: invoice_entity::ActiveModel = model.into();
            am.number = Set(self.number.clone());
            am.status = Set(self.status.clone());
            am.date_issued = Set(self.date_issued.clone());
            am.date_due = Set(self.date_due.clone());
            am.notes = Set(self.notes.clone());
            am.doc_type = Set(self.doc_type.clone());
            am.parent_id = Set(self.parent_id.as_ref().map(|u| u.0.to_vec()));
            am.service_date = Set(self.service_date.clone());
            am.cash_allowance_pct = Set(self.cash_allowance_pct);
            am.cash_allowance_days = Set(self.cash_allowance_days);
            am.discount_pct = Set(self.discount_pct);
            am.payment_method = Set(self.payment_method.clone());
            am.sepa_mandate_id = Set(self.sepa_mandate_id.as_ref().map(|u| u.0.to_vec()));
            am.currency = Set(self.currency.clone());
            am.language = Set(self.language.clone());
            am.project_ref = Set(self.project_ref.clone());
            am.external_id = Set(self.external_id.clone());
            am.billing_street = Set(self.billing_street.clone());
            am.billing_zip = Set(self.billing_zip.clone());
            am.billing_city = Set(self.billing_city.clone());
            am.billing_country = Set(self.billing_country.clone());
            am.shipping_street = Set(self.shipping_street.clone());
            am.shipping_zip = Set(self.shipping_zip.clone());
            am.shipping_city = Set(self.shipping_city.clone());
            am.shipping_country = Set(self.shipping_country.clone());
            am.update(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                "UPDATE invoice {}: {e}", self.id,
            )))?;
            Ok(client_zset.clone())
        }
    }
}
