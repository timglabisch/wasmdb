use sql_engine::storage::Uuid;
use sqlx::Row;
use tables_storage::{query, row};

use crate::{try_uuid, AppCtx, DEMO_TENANT_ID};

#[row(table = "products")]
#[export(name = "", groups = ["all"])]
#[export(name = "WithoutPk", groups = ["non_pk"])]
pub struct Product {
    #[pk]
    pub id: Uuid,
    pub sku: String,
    pub name: String,
    pub description: String,
    pub unit: String,
    pub unit_price: i64,
    pub tax_rate: i64,
    pub cost_price: i64,
    pub active: i64,
}

#[query]
async fn all(ctx: &AppCtx) -> Result<Vec<Product>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT id, sku, name, description, unit, \
         unit_price, tax_rate, cost_price, active \
         FROM invoice_demo.products WHERE tenant_id = ?",
    )
    .bind(DEMO_TENANT_ID)
    .fetch_all(&ctx.pool)
    .await?;

    rows.into_iter()
        .map(|r| {
            Ok(Product {
                id: try_uuid(&r, "id")?,
                sku: r.try_get("sku")?,
                name: r.try_get("name")?,
                description: r.try_get("description")?,
                unit: r.try_get("unit")?,
                unit_price: r.try_get("unit_price")?,
                tax_rate: r.try_get("tax_rate")?,
                cost_price: r.try_get("cost_price")?,
                active: r.try_get("active")?,
            })
        })
        .collect()
}
