//! Server-side: SeaORM Entity for the `invoices` table + `TryFrom<Model>`
//! adapter to the client DTO + `#[query]`-fns. Owns the SQL-side schema.
//!
//! `pub mod entity` is intentionally `pub` so other features (customers
//! cascade, invoice commands) can construct `ActiveModel`s and reference
//! columns when writing to the invoices table.

#![cfg(feature = "server")]

use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use sql_engine::storage::Uuid;
use tables_storage::query;

use super::invoice_client::Invoice;
use crate::shared::DEMO_TENANT_ID;
use crate::AppCtx;

pub mod entity {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(schema_name = "invoice_demo", table_name = "invoices")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false, column_type = "Binary(16)")]
        pub id: Vec<u8>,
        pub tenant_id: i64,
        #[sea_orm(column_type = "Binary(16)", nullable)]
        pub customer_id: Option<Vec<u8>>,
        pub number: String,
        pub status: String,
        pub date_issued: String,
        pub date_due: String,
        #[sea_orm(column_type = "Text")]
        pub notes: String,
        pub doc_type: String,
        #[sea_orm(column_type = "Binary(16)", nullable)]
        pub parent_id: Option<Vec<u8>>,
        pub service_date: String,
        pub cash_allowance_pct: i64,
        pub cash_allowance_days: i64,
        pub discount_pct: i64,
        pub payment_method: String,
        #[sea_orm(column_type = "Binary(16)", nullable)]
        pub sepa_mandate_id: Option<Vec<u8>>,
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

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

impl TryFrom<entity::Model> for Invoice {
    type Error = anyhow::Error;
    fn try_from(m: entity::Model) -> Result<Self, Self::Error> {
        let id_bytes: [u8; 16] = m.id.as_slice().try_into().map_err(|_| {
            anyhow::anyhow!("invoices.id: expected 16 bytes, got {}", m.id.len())
        })?;
        let customer_id = match m.customer_id {
            Some(b) => {
                let arr: [u8; 16] = b.as_slice().try_into().map_err(|_| {
                    anyhow::anyhow!(
                        "invoices.customer_id: expected 16 bytes, got {}",
                        b.len()
                    )
                })?;
                Some(Uuid(arr))
            }
            None => None,
        };
        let parent_id = match m.parent_id {
            Some(b) => {
                let arr: [u8; 16] = b.as_slice().try_into().map_err(|_| {
                    anyhow::anyhow!(
                        "invoices.parent_id: expected 16 bytes, got {}",
                        b.len()
                    )
                })?;
                Some(Uuid(arr))
            }
            None => None,
        };
        let sepa_mandate_id = match m.sepa_mandate_id {
            Some(b) => {
                let arr: [u8; 16] = b.as_slice().try_into().map_err(|_| {
                    anyhow::anyhow!(
                        "invoices.sepa_mandate_id: expected 16 bytes, got {}",
                        b.len()
                    )
                })?;
                Some(Uuid(arr))
            }
            None => None,
        };
        Ok(Invoice {
            id: Uuid(id_bytes),
            customer_id,
            number: m.number,
            status: m.status,
            date_issued: m.date_issued,
            date_due: m.date_due,
            notes: m.notes,
            doc_type: m.doc_type,
            parent_id,
            service_date: m.service_date,
            cash_allowance_pct: m.cash_allowance_pct,
            cash_allowance_days: m.cash_allowance_days,
            discount_pct: m.discount_pct,
            payment_method: m.payment_method,
            sepa_mandate_id,
            currency: m.currency,
            language: m.language,
            project_ref: m.project_ref,
            external_id: m.external_id,
            billing_street: m.billing_street,
            billing_zip: m.billing_zip,
            billing_city: m.billing_city,
            billing_country: m.billing_country,
            shipping_street: m.shipping_street,
            shipping_zip: m.shipping_zip,
            shipping_city: m.shipping_city,
            shipping_country: m.shipping_country,
        })
    }
}

#[query]
async fn all(ctx: &AppCtx) -> anyhow::Result<Vec<Invoice>> {
    let models = entity::Entity::find()
        .filter(entity::Column::TenantId.eq(DEMO_TENANT_ID))
        .all(&ctx.db)
        .await?;
    models.into_iter().map(Invoice::try_from).collect()
}
