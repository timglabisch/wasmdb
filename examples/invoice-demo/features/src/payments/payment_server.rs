//! Server-side: SeaORM Entity for the `payments` table + `TryFrom<Model>`
//! adapter to the client DTO + `#[query]`-fns. Owns the SQL-side schema.
//!
//! `pub mod entity` is intentionally `pub` so other features (customers
//! cascade) can construct `ActiveModel`s and reference columns when
//! writing to the payments table.

#![cfg(feature = "server")]

use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use sql_engine::storage::Uuid;
use tables_storage::query;

use super::payment_client::Payment;
use crate::shared::DEMO_TENANT_ID;
use crate::AppCtx;

pub mod entity {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(schema_name = "invoice_demo", table_name = "payments")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false, column_type = "Binary(16)")]
        pub id: Vec<u8>,
        pub tenant_id: i64,
        #[sea_orm(column_type = "Binary(16)")]
        pub invoice_id: Vec<u8>,
        pub amount: i64,
        pub paid_at: String,
        pub method: String,
        pub reference: String,
        pub note: String,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

impl TryFrom<entity::Model> for Payment {
    type Error = anyhow::Error;
    fn try_from(m: entity::Model) -> Result<Self, Self::Error> {
        let id_bytes: [u8; 16] = m.id.as_slice().try_into().map_err(|_| {
            anyhow::anyhow!("payments.id: expected 16 bytes, got {}", m.id.len())
        })?;
        let invoice_id_bytes: [u8; 16] = m.invoice_id.as_slice().try_into().map_err(|_| {
            anyhow::anyhow!(
                "payments.invoice_id: expected 16 bytes, got {}",
                m.invoice_id.len()
            )
        })?;
        Ok(Payment {
            id: Uuid(id_bytes),
            invoice_id: Uuid(invoice_id_bytes),
            amount: m.amount,
            paid_at: m.paid_at,
            method: m.method,
            reference: m.reference,
            note: m.note,
        })
    }
}

#[query]
async fn all(ctx: &AppCtx) -> anyhow::Result<Vec<Payment>> {
    let models = entity::Entity::find()
        .filter(entity::Column::TenantId.eq(DEMO_TENANT_ID))
        .all(&ctx.db)
        .await?;
    models.into_iter().map(Payment::try_from).collect()
}
