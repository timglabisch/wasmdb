//! Customer row + fetchers. The `#[row]` macro derives Borsh/Serde and
//! `impl Row`. `#[query]` is a validation marker — `Params` structs,
//! `impl Fetcher`, and `register_*` glue are emitted by `tables-codegen`
//! from `build.rs`.

use tables_storage::{query, row};

use crate::AppCtx;

#[row]
pub struct Customer {
    #[pk]
    pub id: i64,
    pub name: String,
}

#[query(id = "invoice_demo::customers::by_owner")]
async fn by_owner(
    owner_id: i64,
    ctx: &AppCtx,
) -> Result<Vec<Customer>, sqlx::Error> {
    let rows: Vec<(i64, String)> = sqlx::query_as(
        "SELECT id, name FROM invoice_demo.customers WHERE owner_id = ?",
    )
    .bind(owner_id)
    .fetch_all(&ctx.pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(id, name)| Customer { id, name })
        .collect())
}
