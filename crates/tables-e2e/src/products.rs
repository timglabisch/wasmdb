//! Product row + queries. PK is `String` (sku); `price` is `Option<i64>`
//! — exercises the `FieldKind::Str` PK path and both `Option<i64>`
//! nullable-column handling + `Option<i64>` arg binding.

use tables_storage::{query, row};

use crate::AppCtx;

#[row]
pub struct Product {
    #[pk]
    pub sku: String,
    pub name: String,
    pub price: Option<i64>,
}

#[query]
async fn by_sku(sku: String, ctx: &AppCtx) -> Result<Vec<Product>, String> {
    Ok(ctx
        .products
        .iter()
        .filter(|p| p.sku == sku)
        .cloned()
        .collect())
}

#[query]
async fn cheaper_than(max_price: i64, ctx: &AppCtx) -> Result<Vec<Product>, String> {
    Ok(ctx
        .products
        .iter()
        .filter(|p| p.price.map_or(false, |pr| pr < max_price))
        .cloned()
        .collect())
}

#[query]
async fn with_optional_price(
    price: Option<i64>,
    ctx: &AppCtx,
) -> Result<Vec<Product>, String> {
    Ok(ctx
        .products
        .iter()
        .filter(|p| p.price == price)
        .cloned()
        .collect())
}
