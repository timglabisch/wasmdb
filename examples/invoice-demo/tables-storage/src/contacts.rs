use sqlx::Row;
use tables_storage::{query, row};

use crate::AppCtx;

#[row(table = "contacts")]
pub struct Contact {
    #[pk]
    pub id: i64,
    pub customer_id: i64,
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
         FROM invoice_demo.contacts",
    )
    .fetch_all(&ctx.pool)
    .await?;

    rows.into_iter()
        .map(|r| {
            Ok(Contact {
                id: r.try_get("id")?,
                customer_id: r.try_get("customer_id")?,
                name: r.try_get("name")?,
                email: r.try_get("email")?,
                phone: r.try_get("phone")?,
                role: r.try_get("role")?,
                is_primary: r.try_get("is_primary")?,
            })
        })
        .collect()
}
