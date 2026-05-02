use database::Database;
use rpc_command::rpc_command;
use sql_engine::storage::Uuid;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

use crate::command_helpers::execute_stmt;
use crate::shared::DEMO_TENANT_ID;

#[rpc_command]
pub struct CreateCustomer {
    #[ts(type = "string")]
    pub id: Uuid,
    pub name: String,
    pub email: String,
    pub created_at: String,
    pub company_type: String,
    pub tax_id: String,
    pub vat_id: String,
    #[ts(type = "number")]
    pub payment_terms_days: i64,
    #[ts(type = "number")]
    pub default_discount_pct: i64,
    pub billing_street: String,
    pub billing_zip: String,
    pub billing_city: String,
    pub billing_country: String,
    pub shipping_street: String,
    pub shipping_zip: String,
    pub shipping_city: String,
    pub shipping_country: String,
    pub default_iban: String,
    pub default_bic: String,
    pub notes: String,
    #[ts(type = "string")]
    #[client_default = "nextId()"]
    pub activity_id: Uuid,
    #[client_default = "new Date().toISOString()"]
    pub timestamp: String,
}

fn detail_for(name: &str) -> String {
    format!("Kunde \"{name}\" angelegt")
}

impl Command for CreateCustomer {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let detail = detail_for(&self.name);
        let mut acc = execute_stmt(
            db,
            sql!(
                "INSERT INTO customers (id, name, email, created_at, company_type, tax_id, vat_id, payment_terms_days, default_discount_pct, billing_street, billing_zip, billing_city, billing_country, shipping_street, shipping_zip, shipping_city, shipping_country, default_iban, default_bic, notes) \
                 VALUES ({self.id}, {self.name}, {self.email}, {self.created_at}, {self.company_type}, {self.tax_id}, {self.vat_id}, {self.payment_terms_days}, {self.default_discount_pct}, {self.billing_street}, {self.billing_zip}, {self.billing_city}, {self.billing_country}, {self.shipping_street}, {self.shipping_zip}, {self.shipping_city}, {self.shipping_country}, {self.default_iban}, {self.default_bic}, {self.notes})"
            ),
        )?;
        acc.extend(execute_stmt(
            db,
            sql!(
                "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
                 VALUES ({self.activity_id}, {self.timestamp}, 'customer', {self.id}, 'create', 'demo', {detail})"
            ),
        )?);
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
    use crate::customers::customer_server::entity as customer_entity;

    #[async_trait]
    impl ServerCommand for CreateCustomer {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let am = customer_entity::ActiveModel {
                tenant_id: Set(DEMO_TENANT_ID),
                id: Set(self.id.0.to_vec()),
                name: Set(self.name.clone()),
                email: Set(self.email.clone()),
                created_at: Set(self.created_at.clone()),
                company_type: Set(self.company_type.clone()),
                tax_id: Set(self.tax_id.clone()),
                vat_id: Set(self.vat_id.clone()),
                payment_terms_days: Set(self.payment_terms_days),
                default_discount_pct: Set(self.default_discount_pct),
                billing_street: Set(self.billing_street.clone()),
                billing_zip: Set(self.billing_zip.clone()),
                billing_city: Set(self.billing_city.clone()),
                billing_country: Set(self.billing_country.clone()),
                shipping_street: Set(self.shipping_street.clone()),
                shipping_zip: Set(self.shipping_zip.clone()),
                shipping_city: Set(self.shipping_city.clone()),
                shipping_country: Set(self.shipping_country.clone()),
                default_iban: Set(self.default_iban.clone()),
                default_bic: Set(self.default_bic.clone()),
                notes: Set(self.notes.clone()),
            };
            customer_entity::Entity::insert(am)
                .on_conflict_do_nothing()
                .exec_without_returning(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "INSERT customer {}: {e}", self.id,
                )))?;

            let detail = detail_for(&self.name);
            insert_activity(
                tx,
                &self.activity_id,
                &self.timestamp,
                "customer",
                &self.id,
                "create",
                &detail,
            )
            .await?;

            Ok(client_zset.clone())
        }
    }
}
