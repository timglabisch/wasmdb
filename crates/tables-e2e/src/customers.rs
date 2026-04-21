//! Customer row + queries. All queries are pure Rust filters over fixtures
//! held in `AppCtx` — no DB, no async I/O — so integration tests stay
//! deterministic and fast.

use tables_storage::{query, row};

use crate::AppCtx;

#[row]
pub struct Customer {
    #[pk]
    pub id: i64,
    pub name: String,
    pub owner_id: i64,
}

#[query]
async fn by_owner(owner_id: i64, ctx: &AppCtx) -> Result<Vec<Customer>, String> {
    Ok(ctx
        .customers
        .iter()
        .filter(|c| c.owner_id == owner_id)
        .cloned()
        .collect())
}

#[query]
async fn by_name(name: String, ctx: &AppCtx) -> Result<Vec<Customer>, String> {
    Ok(ctx
        .customers
        .iter()
        .filter(|c| c.name == name)
        .cloned()
        .collect())
}

#[query]
async fn by_owner_and_name(
    owner_id: i64,
    name: String,
    ctx: &AppCtx,
) -> Result<Vec<Customer>, String> {
    Ok(ctx
        .customers
        .iter()
        .filter(|c| c.owner_id == owner_id && c.name == name)
        .cloned()
        .collect())
}
