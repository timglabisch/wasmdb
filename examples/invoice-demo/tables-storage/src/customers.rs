//! Storage-side fetchers for customer rows. Each `#[storage(Marker)]`
//! generates a `pub fn register_{fn}` to wire into `Registry<AppCtx>`.

use invoice_demo_tables_client::{ByOwner, Customer};
use tables_storage::storage;

use crate::AppCtx;

#[storage]
async fn by_owner(
    params: ByOwner,
    ctx: &AppCtx,
) -> Result<Vec<Customer>, sqlx::Error> {
    let rows: Vec<(i64, String)> = sqlx::query_as(
        "SELECT id, name FROM invoice_demo.customers WHERE owner_id = ?",
    )
    .bind(params.owner_id)
    .fetch_all(&ctx.pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(id, name)| Customer { id, name })
        .collect())
}
