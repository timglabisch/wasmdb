//! Server-side: SeaORM Entity for the `positions` table + `TryFrom<Model>`
//! adapter to the client DTO + `#[query]`-fns. Owns the SQL-side schema.
//!
//! `pub mod entity` is intentionally `pub` so other features (invoices,
//! customers cascade) can construct `ActiveModel`s and reference columns
//! when writing to the positions table.

#![cfg(feature = "server")]

use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use sql_engine::storage::Uuid;
use tables_storage::query;

use super::position_client::Position;
use crate::shared::DEMO_TENANT_ID;
use crate::AppCtx;

pub mod entity {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(schema_name = "invoice_demo", table_name = "positions")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false, column_type = "Binary(16)")]
        pub id: Vec<u8>,
        pub tenant_id: i64,
        #[sea_orm(column_type = "Binary(16)")]
        pub invoice_id: Vec<u8>,
        pub position_nr: i64,
        #[sea_orm(column_type = "Text")]
        pub description: String,
        pub quantity: i64,
        pub unit_price: i64,
        pub tax_rate: i64,
        #[sea_orm(column_type = "Binary(16)", nullable)]
        pub product_id: Option<Vec<u8>>,
        pub item_number: String,
        pub unit: String,
        pub discount_pct: i64,
        pub cost_price: i64,
        pub position_type: String,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

impl TryFrom<entity::Model> for Position {
    type Error = anyhow::Error;
    fn try_from(m: entity::Model) -> Result<Self, Self::Error> {
        let id_bytes: [u8; 16] = m.id.as_slice().try_into().map_err(|_| {
            anyhow::anyhow!("positions.id: expected 16 bytes, got {}", m.id.len())
        })?;
        let invoice_id_bytes: [u8; 16] = m.invoice_id.as_slice().try_into().map_err(|_| {
            anyhow::anyhow!(
                "positions.invoice_id: expected 16 bytes, got {}",
                m.invoice_id.len()
            )
        })?;
        let product_id = match m.product_id {
            Some(bytes) => {
                let arr: [u8; 16] = bytes.as_slice().try_into().map_err(|_| {
                    anyhow::anyhow!(
                        "positions.product_id: expected 16 bytes, got {}",
                        bytes.len()
                    )
                })?;
                Some(Uuid(arr))
            }
            None => None,
        };
        Ok(Position {
            id: Uuid(id_bytes),
            invoice_id: Uuid(invoice_id_bytes),
            position_nr: m.position_nr,
            description: m.description,
            quantity: m.quantity,
            unit_price: m.unit_price,
            tax_rate: m.tax_rate,
            product_id,
            item_number: m.item_number,
            unit: m.unit,
            discount_pct: m.discount_pct,
            cost_price: m.cost_price,
            position_type: m.position_type,
        })
    }
}

#[query]
async fn all(ctx: &AppCtx) -> anyhow::Result<Vec<Position>> {
    let models = entity::Entity::find()
        .filter(entity::Column::TenantId.eq(DEMO_TENANT_ID))
        .all(&ctx.db)
        .await?;
    models.into_iter().map(Position::try_from).collect()
}
