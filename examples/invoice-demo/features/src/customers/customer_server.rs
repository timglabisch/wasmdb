//! Server-side: sqlx-based fetcher for the `customers` table. Owns the
//! SQL-side schema. Unlike `products`, this entity has not been migrated
//! to SeaORM yet — it pulls columns by name because sqlx's `FromRow`
//! tuple impls stop at 16 columns and this row has 20.

#![cfg(feature = "server")]

use sqlx::Row;
use tables_storage::query;

use crate::server_helpers::try_uuid;
use crate::shared::DEMO_TENANT_ID;
use crate::AppCtx;
use super::customer_client::Customer;

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
