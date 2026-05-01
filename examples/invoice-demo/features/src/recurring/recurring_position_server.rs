#![cfg(feature = "server")]

use sqlx::Row;
use tables_storage::query;

use crate::server_helpers::try_uuid;
use crate::shared::DEMO_TENANT_ID;
use crate::AppCtx;
use super::recurring_position_client::RecurringPosition;

#[query]
async fn all(ctx: &AppCtx) -> Result<Vec<RecurringPosition>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT id, recurring_id, position_nr, description, \
         quantity, unit_price, tax_rate, \
         unit, item_number, discount_pct \
         FROM invoice_demo.recurring_positions WHERE tenant_id = ?",
    )
    .bind(DEMO_TENANT_ID)
    .fetch_all(&ctx.pool)
    .await?;

    rows.into_iter()
        .map(|r| {
            Ok(RecurringPosition {
                id: try_uuid(&r, "id")?,
                recurring_id: try_uuid(&r, "recurring_id")?,
                position_nr: r.try_get("position_nr")?,
                description: r.try_get("description")?,
                quantity: r.try_get("quantity")?,
                unit_price: r.try_get("unit_price")?,
                tax_rate: r.try_get("tax_rate")?,
                unit: r.try_get("unit")?,
                item_number: r.try_get("item_number")?,
                discount_pct: r.try_get("discount_pct")?,
            })
        })
        .collect()
}
