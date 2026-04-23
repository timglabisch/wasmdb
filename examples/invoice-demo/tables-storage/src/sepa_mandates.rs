use sqlx::Row;
use tables_storage::{query, row};

use crate::AppCtx;

#[row(table = "sepa_mandates")]
pub struct SepaMandate {
    #[pk]
    pub id: i64,
    pub customer_id: i64,
    pub mandate_ref: String,
    pub iban: String,
    pub bic: String,
    pub holder_name: String,
    pub signed_at: String,
    pub status: String,
}

#[query]
async fn all(ctx: &AppCtx) -> Result<Vec<SepaMandate>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT id, customer_id, mandate_ref, iban, bic, \
         holder_name, signed_at, status \
         FROM invoice_demo.sepa_mandates",
    )
    .fetch_all(&ctx.pool)
    .await?;

    rows.into_iter()
        .map(|r| {
            Ok(SepaMandate {
                id: r.try_get("id")?,
                customer_id: r.try_get("customer_id")?,
                mandate_ref: r.try_get("mandate_ref")?,
                iban: r.try_get("iban")?,
                bic: r.try_get("bic")?,
                holder_name: r.try_get("holder_name")?,
                signed_at: r.try_get("signed_at")?,
                status: r.try_get("status")?,
            })
        })
        .collect()
}
