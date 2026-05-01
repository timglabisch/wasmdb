use sql_engine::storage::Uuid;
use sqlx::Row;
use tables_storage::{query, row};

use crate::{try_uuid, AppCtx, DEMO_TENANT_ID};

#[row(table = "activity_log")]
#[export(name = "", groups = ["all"])]
#[export(name = "WithoutPk", groups = ["non_pk"])]
pub struct ActivityLogEntry {
    #[pk]
    pub id: Uuid,
    pub timestamp: String,
    pub entity_type: String,
    pub entity_id: Uuid,
    pub action: String,
    pub actor: String,
    pub detail: String,
}

#[query]
async fn all(ctx: &AppCtx) -> Result<Vec<ActivityLogEntry>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT id, timestamp, entity_type, entity_id, action, actor, detail \
         FROM invoice_demo.activity_log WHERE tenant_id = ?",
    )
    .bind(DEMO_TENANT_ID)
    .fetch_all(&ctx.pool)
    .await?;

    rows.into_iter()
        .map(|r| {
            Ok(ActivityLogEntry {
                id: try_uuid(&r, "id")?,
                timestamp: r.try_get("timestamp")?,
                entity_type: r.try_get("entity_type")?,
                entity_id: try_uuid(&r, "entity_id")?,
                action: r.try_get("action")?,
                actor: r.try_get("actor")?,
                detail: r.try_get("detail")?,
            })
        })
        .collect()
}
