#![cfg(feature = "server")]

use sqlx::Row;
use tables_storage::query;

use crate::server_helpers::{try_uuid, try_uuid_opt};
use crate::shared::DEMO_TENANT_ID;
use crate::AppCtx;
use super::invoice_client::Invoice;

#[query]
async fn all(ctx: &AppCtx) -> Result<Vec<Invoice>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT id, customer_id, number, status, date_issued, date_due, notes, \
         doc_type, parent_id, service_date, \
         cash_allowance_pct, cash_allowance_days, discount_pct, \
         payment_method, sepa_mandate_id, currency, language, project_ref, external_id, \
         billing_street, billing_zip, billing_city, billing_country, \
         shipping_street, shipping_zip, shipping_city, shipping_country \
         FROM invoice_demo.invoices WHERE tenant_id = ?",
    )
    .bind(DEMO_TENANT_ID)
    .fetch_all(&ctx.pool)
    .await?;

    rows.into_iter()
        .map(|r| {
            Ok(Invoice {
                id: try_uuid(&r, "id")?,
                customer_id: try_uuid_opt(&r, "customer_id")?,
                number: r.try_get("number")?,
                status: r.try_get("status")?,
                date_issued: r.try_get("date_issued")?,
                date_due: r.try_get("date_due")?,
                notes: r.try_get("notes")?,
                doc_type: r.try_get("doc_type")?,
                parent_id: try_uuid_opt(&r, "parent_id")?,
                service_date: r.try_get("service_date")?,
                cash_allowance_pct: r.try_get("cash_allowance_pct")?,
                cash_allowance_days: r.try_get("cash_allowance_days")?,
                discount_pct: r.try_get("discount_pct")?,
                payment_method: r.try_get("payment_method")?,
                sepa_mandate_id: try_uuid_opt(&r, "sepa_mandate_id")?,
                currency: r.try_get("currency")?,
                language: r.try_get("language")?,
                project_ref: r.try_get("project_ref")?,
                external_id: r.try_get("external_id")?,
                billing_street: r.try_get("billing_street")?,
                billing_zip: r.try_get("billing_zip")?,
                billing_city: r.try_get("billing_city")?,
                billing_country: r.try_get("billing_country")?,
                shipping_street: r.try_get("shipping_street")?,
                shipping_zip: r.try_get("shipping_zip")?,
                shipping_city: r.try_get("shipping_city")?,
                shipping_country: r.try_get("shipping_country")?,
            })
        })
        .collect()
}
