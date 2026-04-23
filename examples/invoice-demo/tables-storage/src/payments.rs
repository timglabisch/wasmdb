use sqlx::Row;
use tables_storage::{query, row};

use crate::AppCtx;

#[row(table = "payments")]
pub struct Payment {
    #[pk]
    pub id: i64,
    pub invoice_id: i64,
    pub amount: i64,
    pub paid_at: String,
    pub method: String,
    pub reference: String,
    pub note: String,
}

#[query]
async fn all(ctx: &AppCtx) -> Result<Vec<Payment>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT id, invoice_id, amount, paid_at, method, reference, note \
         FROM invoice_demo.payments",
    )
    .fetch_all(&ctx.pool)
    .await?;

    rows.into_iter()
        .map(|r| {
            Ok(Payment {
                id: r.try_get("id")?,
                invoice_id: r.try_get("invoice_id")?,
                amount: r.try_get("amount")?,
                paid_at: r.try_get("paid_at")?,
                method: r.try_get("method")?,
                reference: r.try_get("reference")?,
                note: r.try_get("note")?,
            })
        })
        .collect()
}
