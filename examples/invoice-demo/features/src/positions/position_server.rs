#![cfg(feature = "server")]

use sqlx::Row;
use tables_storage::query;

use crate::server_helpers::{try_uuid, try_uuid_opt};
use crate::shared::DEMO_TENANT_ID;
use crate::AppCtx;
use super::position_client::Position;

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
                product_id: try_uuid_opt(&r, "product_id")?,
                item_number: r.try_get("item_number")?,
                unit: r.try_get("unit")?,
                discount_pct: r.try_get("discount_pct")?,
                cost_price: r.try_get("cost_price")?,
                position_type: r.try_get("position_type")?,
            })
        })
        .collect()
}
