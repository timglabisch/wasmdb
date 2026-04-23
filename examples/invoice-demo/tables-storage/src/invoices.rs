use sqlx::Row;
use tables_storage::{query, row};

use crate::AppCtx;

#[row(table = "invoices")]
pub struct Invoice {
    #[pk]
    pub id: i64,
    pub customer_id: i64,
    pub number: String,
    pub status: String,
    pub date_issued: String,
    pub date_due: String,
    pub notes: String,
    pub doc_type: String,
    pub parent_id: i64,
    pub service_date: String,
    pub cash_allowance_pct: i64,
    pub cash_allowance_days: i64,
    pub discount_pct: i64,
    pub payment_method: String,
    pub sepa_mandate_id: i64,
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

#[query]
async fn all(ctx: &AppCtx) -> Result<Vec<Invoice>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT id, customer_id, number, status, date_issued, date_due, notes, \
         doc_type, parent_id, service_date, \
         cash_allowance_pct, cash_allowance_days, discount_pct, \
         payment_method, sepa_mandate_id, currency, language, project_ref, external_id, \
         billing_street, billing_zip, billing_city, billing_country, \
         shipping_street, shipping_zip, shipping_city, shipping_country \
         FROM invoice_demo.invoices",
    )
    .fetch_all(&ctx.pool)
    .await?;

    rows.into_iter()
        .map(|r| {
            Ok(Invoice {
                id: r.try_get("id")?,
                customer_id: r.try_get("customer_id")?,
                number: r.try_get("number")?,
                status: r.try_get("status")?,
                date_issued: r.try_get("date_issued")?,
                date_due: r.try_get("date_due")?,
                notes: r.try_get("notes")?,
                doc_type: r.try_get("doc_type")?,
                parent_id: r.try_get("parent_id")?,
                service_date: r.try_get("service_date")?,
                cash_allowance_pct: r.try_get("cash_allowance_pct")?,
                cash_allowance_days: r.try_get("cash_allowance_days")?,
                discount_pct: r.try_get("discount_pct")?,
                payment_method: r.try_get("payment_method")?,
                sepa_mandate_id: r.try_get("sepa_mandate_id")?,
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
