use database::Database;
use rpc_command::rpc_command;
use sql_engine::execute::Params;
use sql_engine::storage::Uuid;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;

use crate::command_helpers::{execute_sql, p_int, p_str, p_uuid};
use crate::shared::DEMO_TENANT_ID;

#[rpc_command]
pub struct UpdateCustomer {
    #[ts(type = "string")]
    pub id: Uuid,
    pub name: String,
    pub email: String,
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
}

impl Command for UpdateCustomer {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let params = Params::from([
            p_uuid("id", &self.id),
            p_str("name", &self.name),
            p_str("email", &self.email),
            p_str("company_type", &self.company_type),
            p_str("tax_id", &self.tax_id),
            p_str("vat_id", &self.vat_id),
            p_int("payment_terms_days", self.payment_terms_days),
            p_int("default_discount_pct", self.default_discount_pct),
            p_str("billing_street", &self.billing_street),
            p_str("billing_zip", &self.billing_zip),
            p_str("billing_city", &self.billing_city),
            p_str("billing_country", &self.billing_country),
            p_str("shipping_street", &self.shipping_street),
            p_str("shipping_zip", &self.shipping_zip),
            p_str("shipping_city", &self.shipping_city),
            p_str("shipping_country", &self.shipping_country),
            p_str("default_iban", &self.default_iban),
            p_str("default_bic", &self.default_bic),
            p_str("notes", &self.notes),
        ]);
        execute_sql(db,
            "UPDATE customers SET name = :name, email = :email, \
             company_type = :company_type, tax_id = :tax_id, vat_id = :vat_id, \
             payment_terms_days = :payment_terms_days, default_discount_pct = :default_discount_pct, \
             billing_street = :billing_street, billing_zip = :billing_zip, billing_city = :billing_city, billing_country = :billing_country, \
             shipping_street = :shipping_street, shipping_zip = :shipping_zip, shipping_city = :shipping_city, shipping_country = :shipping_country, \
             default_iban = :default_iban, default_bic = :default_bic, notes = :notes \
             WHERE customers.id = :id",
            params)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter, Set};
    use sync_server_mysql::ServerCommand;

    use crate::customers::customer_server::entity as customer_entity;

    #[async_trait]
    impl ServerCommand for UpdateCustomer {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let model = customer_entity::Entity::find()
                .filter(customer_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(customer_entity::Column::Id.eq(self.id.0.to_vec()))
                .one(tx).await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "load customer {}: {e}", self.id,
                )))?
                .ok_or_else(|| CommandError::ExecutionFailed(format!(
                    "customer {} not found", self.id,
                )))?;

            let mut am: customer_entity::ActiveModel = model.into();
            am.name = Set(self.name.clone());
            am.email = Set(self.email.clone());
            am.company_type = Set(self.company_type.clone());
            am.tax_id = Set(self.tax_id.clone());
            am.vat_id = Set(self.vat_id.clone());
            am.payment_terms_days = Set(self.payment_terms_days);
            am.default_discount_pct = Set(self.default_discount_pct);
            am.billing_street = Set(self.billing_street.clone());
            am.billing_zip = Set(self.billing_zip.clone());
            am.billing_city = Set(self.billing_city.clone());
            am.billing_country = Set(self.billing_country.clone());
            am.shipping_street = Set(self.shipping_street.clone());
            am.shipping_zip = Set(self.shipping_zip.clone());
            am.shipping_city = Set(self.shipping_city.clone());
            am.shipping_country = Set(self.shipping_country.clone());
            am.default_iban = Set(self.default_iban.clone());
            am.default_bic = Set(self.default_bic.clone());
            am.notes = Set(self.notes.clone());
            am.update(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                "UPDATE customer {}: {e}", self.id,
            )))?;
            Ok(client_zset.clone())
        }
    }
}
