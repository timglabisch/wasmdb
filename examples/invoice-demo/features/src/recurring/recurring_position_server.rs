//! Server-side: SeaORM Entity for the `recurring_positions` table + `TryFrom<Model>`
//! adapter to the client DTO + `#[query]`-fns. Owns the SQL-side schema.
//!
//! `entity` is `pub` so other features (cascades, run_recurring_once)
//! can reach in.

#![cfg(feature = "server")]

use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use sql_engine::storage::Uuid;
use tables_storage::query;

use super::recurring_position_client::RecurringPosition;
use crate::shared::DEMO_TENANT_ID;
use crate::AppCtx;

pub mod entity {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(schema_name = "invoice_demo", table_name = "recurring_positions")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub tenant_id: i64,
        #[sea_orm(primary_key, auto_increment = false, column_type = "Binary(16)")]
        pub id: Vec<u8>,
        #[sea_orm(column_type = "Binary(16)")]
        pub recurring_id: Vec<u8>,
        pub position_nr: i64,
        #[sea_orm(column_type = "Text")]
        pub description: String,
        pub quantity: i64,
        pub unit_price: i64,
        pub tax_rate: i64,
        pub unit: String,
        pub item_number: String,
        pub discount_pct: i64,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

impl TryFrom<entity::Model> for RecurringPosition {
    type Error = anyhow::Error;
    fn try_from(m: entity::Model) -> Result<Self, Self::Error> {
        let id_bytes: [u8; 16] = m.id.as_slice().try_into().map_err(|_| {
            anyhow::anyhow!("recurring_positions.id: expected 16 bytes, got {}", m.id.len())
        })?;
        let recurring_id_bytes: [u8; 16] = m.recurring_id.as_slice().try_into().map_err(|_| {
            anyhow::anyhow!(
                "recurring_positions.recurring_id: expected 16 bytes, got {}",
                m.recurring_id.len()
            )
        })?;
        Ok(RecurringPosition {
            id: Uuid(id_bytes),
            recurring_id: Uuid(recurring_id_bytes),
            position_nr: m.position_nr,
            description: m.description,
            quantity: m.quantity,
            unit_price: m.unit_price,
            tax_rate: m.tax_rate,
            unit: m.unit,
            item_number: m.item_number,
            discount_pct: m.discount_pct,
        })
    }
}

#[query]
async fn all(ctx: &AppCtx) -> anyhow::Result<Vec<RecurringPosition>> {
    let models = entity::Entity::find()
        .filter(entity::Column::TenantId.eq(DEMO_TENANT_ID))
        .all(&ctx.db)
        .await?;
    models.into_iter().map(RecurringPosition::try_from).collect()
}
