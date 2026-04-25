use sql_engine::storage::Uuid;
use sqlx::Row;
use tables_storage::{query, row};

use crate::{try_uuid, AppCtx, DEMO_TENANT_ID};

#[row(table = "positions")]
pub struct Position {
    #[pk]
    pub id: Uuid,
    pub invoice_id: Uuid,
    pub position_nr: i64,
    pub description: String,
    pub quantity: i64,
    pub unit_price: i64,
    pub tax_rate: i64,
    pub product_id: Uuid,
    pub item_number: String,
    pub unit: String,
    pub discount_pct: i64,
    pub cost_price: i64,
    pub position_type: String,
}

#[query]
async fn all(ctx: &AppCtx) -> Result<Vec<Position>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT id, invoice_id, position_nr, description, \
         quantity, unit_price, tax_rate, product_id, \
         item_number, unit, discount_pct, cost_price, position_type \
         FROM invoice_demo.positions WHERE tenant_id = ?",
    )
    .bind(DEMO_TENANT_ID)
    .fetch_all(&ctx.pool)
    .await?;

    rows.into_iter()
        .map(|r| {
            Ok(Position {
                id: try_uuid(&r, "id")?,
                invoice_id: try_uuid(&r, "invoice_id")?,
                position_nr: r.try_get("position_nr")?,
                description: r.try_get("description")?,
                quantity: r.try_get("quantity")?,
                unit_price: r.try_get("unit_price")?,
                tax_rate: r.try_get("tax_rate")?,
                product_id: try_uuid(&r, "product_id")?,
                item_number: r.try_get("item_number")?,
                unit: r.try_get("unit")?,
                discount_pct: r.try_get("discount_pct")?,
                cost_price: r.try_get("cost_price")?,
                position_type: r.try_get("position_type")?,
            })
        })
        .collect()
}
