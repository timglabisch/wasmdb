use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use sql_engine::storage::Uuid;
use ts_rs::TS;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str, p_uuid};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
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
}

impl Command for CreateCustomer {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let params = Params::from([
            p_uuid("id", &self.id),
            p_str("name", &self.name),
            p_str("email", &self.email),
            p_str("created_at", &self.created_at),
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
            "INSERT INTO customers (id, name, email, created_at, company_type, tax_id, vat_id, payment_terms_days, default_discount_pct, billing_street, billing_zip, billing_city, billing_country, shipping_street, shipping_zip, shipping_city, shipping_country, default_iban, default_bic, notes) \
             VALUES (:id, :name, :email, :created_at, :company_type, :tax_id, :vat_id, :payment_terms_days, :default_discount_pct, :billing_street, :billing_zip, :billing_city, :billing_country, :shipping_street, :shipping_zip, :shipping_city, :shipping_country, :default_iban, :default_bic, :notes)",
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
    impl ServerCommand for CreateCustomer {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            sqlx::query(
                "INSERT INTO customers (id, name, email, created_at, company_type, tax_id, vat_id, payment_terms_days, default_discount_pct, billing_street, billing_zip, billing_city, billing_country, shipping_street, shipping_zip, shipping_city, shipping_country, default_iban, default_bic, notes) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
                .bind(&self.id.0[..])
                .bind(&self.name)
                .bind(&self.email)
                .bind(&self.created_at)
                .bind(&self.company_type)
                .bind(&self.tax_id)
                .bind(&self.vat_id)
                .bind(self.payment_terms_days)
                .bind(self.default_discount_pct)
                .bind(&self.billing_street)
                .bind(&self.billing_zip)
                .bind(&self.billing_city)
                .bind(&self.billing_country)
                .bind(&self.shipping_street)
                .bind(&self.shipping_zip)
                .bind(&self.shipping_city)
                .bind(&self.shipping_country)
                .bind(&self.default_iban)
                .bind(&self.default_bic)
                .bind(&self.notes)
                .execute(&mut **tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "INSERT customer {}: {e}",
                    self.id,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
