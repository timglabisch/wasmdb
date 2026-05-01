#![cfg(feature = "server")]

use sqlx::Row;
use tables_storage::query;

use crate::server_helpers::try_uuid;
use crate::shared::DEMO_TENANT_ID;
use crate::AppCtx;
use super::contact_client::Contact;

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
