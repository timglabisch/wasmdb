//! Server-side: SeaORM Entity for the `recurring_invoices` table + `TryFrom<Model>`
//! adapter to the client DTO + `#[query]`-fns. Owns the SQL-side schema.
//!
//! `entity` is `pub` so other features (cascades, run_recurring_once)
//! can reach in.

#![cfg(feature = "server")]

use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use sql_engine::storage::Uuid;
use tables_storage::query;

use super::recurring_invoice_client::RecurringInvoice;
use crate::shared::DEMO_TENANT_ID;
use crate::AppCtx;

pub mod entity {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(schema_name = "invoice_demo", table_name = "recurring_invoices")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub tenant_id: i64,
        #[sea_orm(primary_key, auto_increment = false, column_type = "Binary(16)")]
        pub id: Vec<u8>,
        #[sea_orm(column_type = "Binary(16)")]
        pub customer_id: Vec<u8>,
        pub template_name: String,
        pub interval_unit: String,
        pub interval_value: i64,
        pub next_run: String,
        pub last_run: String,
        pub enabled: i64,
        pub status_template: String,
        #[sea_orm(column_type = "Text")]
        pub notes_template: String,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

impl TryFrom<entity::Model> for RecurringInvoice {
    type Error = anyhow::Error;
    fn try_from(m: entity::Model) -> Result<Self, Self::Error> {
        let id_bytes: [u8; 16] = m.id.as_slice().try_into().map_err(|_| {
            anyhow::anyhow!("recurring_invoices.id: expected 16 bytes, got {}", m.id.len())
        })?;
        let customer_id_bytes: [u8; 16] = m.customer_id.as_slice().try_into().map_err(|_| {
            anyhow::anyhow!(
                "recurring_invoices.customer_id: expected 16 bytes, got {}",
                m.customer_id.len()
            )
        })?;
        Ok(RecurringInvoice {
            id: Uuid(id_bytes),
            customer_id: Uuid(customer_id_bytes),
            template_name: m.template_name,
            interval_unit: m.interval_unit,
            interval_value: m.interval_value,
            next_run: m.next_run,
            last_run: m.last_run,
            enabled: m.enabled,
            status_template: m.status_template,
            notes_template: m.notes_template,
        })
    }
}

#[query]
async fn all(ctx: &AppCtx) -> anyhow::Result<Vec<RecurringInvoice>> {
    let models = entity::Entity::find()
        .filter(entity::Column::TenantId.eq(DEMO_TENANT_ID))
        .all(&ctx.db)
        .await?;
    models.into_iter().map(RecurringInvoice::try_from).collect()
}
