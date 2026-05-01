use sql_engine::storage::Uuid;
use sqlx::Row;
use tables_storage::{query, row};

use crate::{try_uuid, AppCtx, DEMO_TENANT_ID};

#[row(table = "contacts")]
#[export(name = "", groups = ["all"])]
#[export(name = "WithoutPk", groups = ["non_pk"])]
pub struct Contact {
    #[pk]
    pub id: Uuid,
    pub customer_id: Uuid,
    pub name: String,
    pub email: String,
    pub phone: String,
    pub role: String,
    pub is_primary: i64,
}

#[query]
async fn all(ctx: &AppCtx) -> Result<Vec<Contact>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT id, customer_id, name, email, phone, role, is_primary \
         FROM invoice_demo.contacts WHERE tenant_id = ?",
    )
    .bind(DEMO_TENANT_ID)
    .fetch_all(&ctx.pool)
    .await?;

    rows.into_iter()
        .map(|r| {
            Ok(Contact {
                id: try_uuid(&r, "id")?,
                customer_id: try_uuid(&r, "customer_id")?,
                name: r.try_get("name")?,
                email: r.try_get("email")?,
                phone: r.try_get("phone")?,
                role: r.try_get("role")?,
                is_primary: r.try_get("is_primary")?,
            })
        })
        .collect()
}
