use sql_engine::storage::Uuid;
use database::Database;
use rpc_command::rpc_command;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::shared::DEMO_TENANT_ID;

#[rpc_command]
pub struct CreateInvoice {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "string | null")]
    pub customer_id: Option<Uuid>,
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
    #[ts(type = "string")]
    #[client_default = "nextId()"]
    pub activity_id: Uuid,
    #[client_default = "new Date().toISOString()"]
    pub timestamp: String,
}

fn detail_for(number: &str) -> String {
    format!("Rechnung \"{number}\" angelegt (Entwurf)")
}

impl Command for CreateInvoice {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let detail = detail_for(&self.number);
        let mut acc = sql!(
            "INSERT INTO invoices (id, customer_id, number, status, date_issued, date_due, notes, doc_type, parent_id, service_date, cash_allowance_pct, cash_allowance_days, discount_pct, payment_method, sepa_mandate_id, currency, language, project_ref, external_id, billing_street, billing_zip, billing_city, billing_country, shipping_street, shipping_zip, shipping_city, shipping_country) \
             VALUES ({self.id}, {self.customer_id}, {self.number}, {self.status}, {self.date_issued}, {self.date_due}, {self.notes}, {self.doc_type}, {self.parent_id}, {self.service_date}, {self.cash_allowance_pct}, {self.cash_allowance_days}, {self.discount_pct}, {self.payment_method}, {self.sepa_mandate_id}, {self.currency}, {self.language}, {self.project_ref}, {self.external_id}, {self.billing_street}, {self.billing_zip}, {self.billing_city}, {self.billing_country}, {self.shipping_street}, {self.shipping_zip}, {self.shipping_city}, {self.shipping_country})"
        )
        .execute(db)?;
        acc.extend(
            sql!(
                "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
                 VALUES ({self.activity_id}, {self.timestamp}, 'invoice', {self.id}, 'create', 'demo', {detail})"
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
    use sea_orm::{DatabaseTransaction, EntityTrait, Set};
    use sync_server_mysql::ServerCommand;

    use crate::activity_log::activity_log_server::insert_activity;
    use crate::invoices::invoice_server::entity as invoice_entity;

    #[async_trait]
    impl ServerCommand for CreateInvoice {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let am = invoice_entity::ActiveModel {
                tenant_id: Set(DEMO_TENANT_ID),
                id: Set(self.id.0.to_vec()),
                customer_id: Set(self.customer_id.as_ref().map(|u| u.0.to_vec())),
                number: Set(self.number.clone()),
                status: Set(self.status.clone()),
                date_issued: Set(self.date_issued.clone()),
                date_due: Set(self.date_due.clone()),
                notes: Set(self.notes.clone()),
                doc_type: Set(self.doc_type.clone()),
                parent_id: Set(self.parent_id.as_ref().map(|u| u.0.to_vec())),
                service_date: Set(self.service_date.clone()),
                cash_allowance_pct: Set(self.cash_allowance_pct),
                cash_allowance_days: Set(self.cash_allowance_days),
                discount_pct: Set(self.discount_pct),
                payment_method: Set(self.payment_method.clone()),
                sepa_mandate_id: Set(self.sepa_mandate_id.as_ref().map(|u| u.0.to_vec())),
                currency: Set(self.currency.clone()),
                language: Set(self.language.clone()),
                project_ref: Set(self.project_ref.clone()),
                external_id: Set(self.external_id.clone()),
                billing_street: Set(self.billing_street.clone()),
                billing_zip: Set(self.billing_zip.clone()),
                billing_city: Set(self.billing_city.clone()),
                billing_country: Set(self.billing_country.clone()),
                shipping_street: Set(self.shipping_street.clone()),
                shipping_zip: Set(self.shipping_zip.clone()),
                shipping_city: Set(self.shipping_city.clone()),
                shipping_country: Set(self.shipping_country.clone()),
            };
            invoice_entity::Entity::insert(am)
                .on_conflict_do_nothing()
                .exec_without_returning(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "INSERT invoice {}: {e}", self.id,
                )))?;

            let detail = detail_for(&self.number);
            insert_activity(
                tx,
                &self.activity_id,
                &self.timestamp,
                "invoice",
                &self.id,
                "create",
                &detail,
            )
            .await?;

            Ok(client_zset.clone())
        }
    }
}
