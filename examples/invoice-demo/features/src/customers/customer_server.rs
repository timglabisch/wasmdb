//! Server-side: SeaORM Entity for the `customers` table + `From<Model>`
//! adapter to the client DTO + `#[query]`-fns. Owns the SQL-side schema.

#![cfg(feature = "server")]

use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use sql_engine::storage::Uuid;
use tables_storage::query;

use crate::shared::DEMO_TENANT_ID;
use crate::AppCtx;
use super::customer_client::Customer;

pub mod entity {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(schema_name = "invoice_demo", table_name = "customers")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false, column_type = "Binary(16)")]
        pub id: Vec<u8>,
        pub tenant_id: i64,
        pub name: String,
        pub email: String,
        pub created_at: String,
        pub company_type: String,
        pub tax_id: String,
        pub vat_id: String,
        pub payment_terms_days: i64,
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
        #[sea_orm(column_type = "Text")]
        pub notes: String,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

impl TryFrom<entity::Model> for Customer {
    type Error = anyhow::Error;
    fn try_from(m: entity::Model) -> Result<Self, Self::Error> {
        let id_bytes: [u8; 16] = m.id.as_slice().try_into().map_err(|_| {
            anyhow::anyhow!("customers.id: expected 16 bytes, got {}", m.id.len())
        })?;
        Ok(Customer {
            id: Uuid(id_bytes),
            name: m.name,
            email: m.email,
            created_at: m.created_at,
            company_type: m.company_type,
            tax_id: m.tax_id,
            vat_id: m.vat_id,
            payment_terms_days: m.payment_terms_days,
            default_discount_pct: m.default_discount_pct,
            billing_street: m.billing_street,
            billing_zip: m.billing_zip,
            billing_city: m.billing_city,
            billing_country: m.billing_country,
            shipping_street: m.shipping_street,
            shipping_zip: m.shipping_zip,
            shipping_city: m.shipping_city,
            shipping_country: m.shipping_country,
            default_iban: m.default_iban,
            default_bic: m.default_bic,
            notes: m.notes,
        })
    }
}

#[query]
async fn all(ctx: &AppCtx) -> anyhow::Result<Vec<Customer>> {
    let models = entity::Entity::find()
        .filter(entity::Column::TenantId.eq(DEMO_TENANT_ID))
        .all(&ctx.db)
        .await?;
    models.into_iter().map(Customer::try_from).collect()
}
