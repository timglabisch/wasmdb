use sql_engine::storage::Uuid;
use sqlx::Row;
use tables_storage::{query, row};

use crate::{try_uuid, AppCtx, DEMO_TENANT_ID};

#[row(table = "sepa_mandates")]
pub struct SepaMandate {
    #[pk]
    pub id: Uuid,
    pub customer_id: Uuid,
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
         FROM invoice_demo.sepa_mandates WHERE tenant_id = ?",
    )
    .bind(DEMO_TENANT_ID)
    .fetch_all(&ctx.pool)
    .await?;

    rows.into_iter()
        .map(|r| {
            Ok(SepaMandate {
                id: try_uuid(&r, "id")?,
                customer_id: try_uuid(&r, "customer_id")?,
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
