use sqlx::Row;
use tables_storage::{query, row};

use crate::AppCtx;

#[row(table = "activity_log")]
pub struct ActivityLogEntry {
    #[pk]
    pub id: i64,
    pub timestamp: String,
    pub entity_type: String,
    pub entity_id: i64,
    pub action: String,
    pub actor: String,
    pub detail: String,
}

#[query]
async fn all(ctx: &AppCtx) -> Result<Vec<ActivityLogEntry>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT id, timestamp, entity_type, entity_id, action, actor, detail \
         FROM invoice_demo.activity_log",
    )
    .fetch_all(&ctx.pool)
    .await?;

    rows.into_iter()
        .map(|r| {
            Ok(ActivityLogEntry {
                id: r.try_get("id")?,
                timestamp: r.try_get("timestamp")?,
                entity_type: r.try_get("entity_type")?,
                entity_id: r.try_get("entity_id")?,
                action: r.try_get("action")?,
                actor: r.try_get("actor")?,
                detail: r.try_get("detail")?,
            })
        })
        .collect()
}
