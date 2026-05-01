#![cfg(feature = "server")]

use sqlx::Row;
use tables_storage::query;

use crate::server_helpers::try_uuid;
use crate::shared::DEMO_TENANT_ID;
use crate::AppCtx;
use super::recurring_invoice_client::RecurringInvoice;

#[query]
async fn all(ctx: &AppCtx) -> Result<Vec<RecurringInvoice>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT id, customer_id, template_name, \
         interval_unit, interval_value, next_run, last_run, \
         enabled, status_template, notes_template \
         FROM invoice_demo.recurring_invoices WHERE tenant_id = ?",
    )
    .bind(DEMO_TENANT_ID)
    .fetch_all(&ctx.pool)
    .await?;

    rows.into_iter()
        .map(|r| {
            Ok(RecurringInvoice {
                id: try_uuid(&r, "id")?,
                customer_id: try_uuid(&r, "customer_id")?,
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
