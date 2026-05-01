#![cfg(feature = "server")]

use sqlx::Row;
use tables_storage::query;

use crate::server_helpers::try_uuid;
use crate::shared::DEMO_TENANT_ID;
use crate::AppCtx;
use super::payment_client::Payment;

#[query]
async fn all(ctx: &AppCtx) -> Result<Vec<Payment>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT id, invoice_id, amount, paid_at, method, reference, note \
         FROM invoice_demo.payments WHERE tenant_id = ?",
    )
    .bind(DEMO_TENANT_ID)
    .fetch_all(&ctx.pool)
    .await?;

    rows.into_iter()
        .map(|r| {
            Ok(Payment {
                id: try_uuid(&r, "id")?,
                invoice_id: try_uuid(&r, "invoice_id")?,
                amount: r.try_get("amount")?,
                paid_at: r.try_get("paid_at")?,
                method: r.try_get("method")?,
                reference: r.try_get("reference")?,
                note: r.try_get("note")?,
            })
        })
        .collect()
}
