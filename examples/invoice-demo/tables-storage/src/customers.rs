//! Customer row + fetchers. The `#[row]` macro derives Borsh/Serde and
//! `impl Row`. `#[query]` is a validation marker — `Params` structs,
//! `impl Fetcher`, and `register_*` glue are emitted by `tables-codegen`
//! from `build.rs`.
//!
//! Column order mirrors `sql/001_init.sql` and
//! `server/src/schema/customers.rs` exactly — Phase-0 upsert ships cells
//! in this order and mismatches silently corrupt the local table.

use sql_engine::storage::Uuid;
use sqlx::Row;
use tables_storage::{query, row};

use crate::{try_uuid, AppCtx, DEMO_TENANT_ID};

#[row(table = "customers")]
#[export(name = "", groups = ["all"])]
#[export(name = "WithoutPk", groups = ["non_pk"])]
pub struct Customer {
    #[pk]
    pub id: Uuid,
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
    pub notes: String,
}

#[query]
async fn all(ctx: &AppCtx) -> Result<Vec<Customer>, sqlx::Error> {
    // sqlx's `FromRow` tuple impls stop at 16 columns; this row has 20.
    // Pull columns by name instead.
    let rows = sqlx::query(
        "SELECT id, name, email, created_at, company_type, tax_id, vat_id, \
         payment_terms_days, default_discount_pct, \
         billing_street, billing_zip, billing_city, billing_country, \
         shipping_street, shipping_zip, shipping_city, shipping_country, \
         default_iban, default_bic, notes \
         FROM invoice_demo.customers WHERE tenant_id = ?",
    )
    .bind(DEMO_TENANT_ID)
    .fetch_all(&ctx.pool)
    .await?;

    rows.into_iter()
        .map(|r| {
            Ok(Customer {
                id: try_uuid(&r, "id")?,
                name: r.try_get("name")?,
                email: r.try_get("email")?,
                created_at: r.try_get("created_at")?,
                company_type: r.try_get("company_type")?,
                tax_id: r.try_get("tax_id")?,
                vat_id: r.try_get("vat_id")?,
                payment_terms_days: r.try_get("payment_terms_days")?,
                default_discount_pct: r.try_get("default_discount_pct")?,
                billing_street: r.try_get("billing_street")?,
                billing_zip: r.try_get("billing_zip")?,
                billing_city: r.try_get("billing_city")?,
                billing_country: r.try_get("billing_country")?,
                shipping_street: r.try_get("shipping_street")?,
                shipping_zip: r.try_get("shipping_zip")?,
                shipping_city: r.try_get("shipping_city")?,
                shipping_country: r.try_get("shipping_country")?,
                default_iban: r.try_get("default_iban")?,
                default_bic: r.try_get("default_bic")?,
                notes: r.try_get("notes")?,
            })
        })
        .collect()
}
