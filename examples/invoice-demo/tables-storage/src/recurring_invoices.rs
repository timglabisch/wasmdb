use sqlx::Row;
use tables_storage::{query, row};

use crate::AppCtx;

#[row(table = "recurring_invoices")]
pub struct RecurringInvoice {
    #[pk]
    pub id: i64,
    pub customer_id: i64,
    pub template_name: String,
    pub interval_unit: String,
    pub interval_value: i64,
    pub next_run: String,
    pub last_run: String,
    pub enabled: i64,
    pub status_template: String,
    pub notes_template: String,
}

#[query]
async fn all(ctx: &AppCtx) -> Result<Vec<RecurringInvoice>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT id, customer_id, template_name, \
         interval_unit, interval_value, next_run, last_run, \
         enabled, status_template, notes_template \
         FROM invoice_demo.recurring_invoices",
    )
    .fetch_all(&ctx.pool)
    .await?;

    rows.into_iter()
        .map(|r| {
            Ok(RecurringInvoice {
                id: r.try_get("id")?,
                customer_id: r.try_get("customer_id")?,
                template_name: r.try_get("template_name")?,
                interval_unit: r.try_get("interval_unit")?,
                interval_value: r.try_get("interval_value")?,
                next_run: r.try_get("next_run")?,
                last_run: r.try_get("last_run")?,
                enabled: r.try_get("enabled")?,
                status_template: r.try_get("status_template")?,
                notes_template: r.try_get("notes_template")?,
            })
        })
        .collect()
}
