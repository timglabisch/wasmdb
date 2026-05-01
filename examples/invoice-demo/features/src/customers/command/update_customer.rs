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
    use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter};
    use sync_server_mysql::ServerCommand;

    use crate::customers::customer_server::entity as customer_entity;

    #[async_trait]
    impl ServerCommand for UpdateCustomer {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            customer_entity::Entity::update_many()
                .col_expr(customer_entity::Column::Name, self.name.clone().into())
                .col_expr(customer_entity::Column::Email, self.email.clone().into())
                .col_expr(customer_entity::Column::CompanyType, self.company_type.clone().into())
                .col_expr(customer_entity::Column::TaxId, self.tax_id.clone().into())
                .col_expr(customer_entity::Column::VatId, self.vat_id.clone().into())
                .col_expr(customer_entity::Column::PaymentTermsDays, self.payment_terms_days.into())
                .col_expr(customer_entity::Column::DefaultDiscountPct, self.default_discount_pct.into())
                .col_expr(customer_entity::Column::BillingStreet, self.billing_street.clone().into())
                .col_expr(customer_entity::Column::BillingZip, self.billing_zip.clone().into())
                .col_expr(customer_entity::Column::BillingCity, self.billing_city.clone().into())
                .col_expr(customer_entity::Column::BillingCountry, self.billing_country.clone().into())
                .col_expr(customer_entity::Column::ShippingStreet, self.shipping_street.clone().into())
                .col_expr(customer_entity::Column::ShippingZip, self.shipping_zip.clone().into())
                .col_expr(customer_entity::Column::ShippingCity, self.shipping_city.clone().into())
                .col_expr(customer_entity::Column::ShippingCountry, self.shipping_country.clone().into())
                .col_expr(customer_entity::Column::DefaultIban, self.default_iban.clone().into())
                .col_expr(customer_entity::Column::DefaultBic, self.default_bic.clone().into())
                .col_expr(customer_entity::Column::Notes, self.notes.clone().into())
                .filter(customer_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(customer_entity::Column::Id.eq(self.id.0.to_vec()))
                .exec(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "UPDATE customer {}: {e}", self.id,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
