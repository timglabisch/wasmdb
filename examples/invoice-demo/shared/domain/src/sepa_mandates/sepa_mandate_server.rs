//! Server-side: SeaORM Entity for the `sepa_mandates` table + `TryFrom<Model>`
//! adapter to the client DTO + `#[query]`-fns. Owns the SQL-side schema.
//!
//! `entity` is `pub` so other features (e.g. customers cascade) can reach in.

#![cfg(feature = "server")]

use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use sql_engine::storage::Uuid;
use tables_storage::query;

use super::sepa_mandate_client::SepaMandate;
use crate::shared::DEMO_TENANT_ID;
use crate::AppCtx;

pub mod entity {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(schema_name = "invoice_demo", table_name = "sepa_mandates")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub tenant_id: i64,
        #[sea_orm(primary_key, auto_increment = false, column_type = "Binary(16)")]
        pub id: Vec<u8>,
        #[sea_orm(column_type = "Binary(16)")]
        pub customer_id: Vec<u8>,
        pub mandate_ref: String,
        pub iban: String,
        pub bic: String,
        pub holder_name: String,
        pub signed_at: String,
        pub status: String,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

impl TryFrom<entity::Model> for SepaMandate {
    type Error = anyhow::Error;
    fn try_from(m: entity::Model) -> Result<Self, Self::Error> {
        let id_bytes: [u8; 16] = m.id.as_slice().try_into().map_err(|_| {
            anyhow::anyhow!("sepa_mandates.id: expected 16 bytes, got {}", m.id.len())
        })?;
        let customer_id_bytes: [u8; 16] = m.customer_id.as_slice().try_into().map_err(|_| {
            anyhow::anyhow!(
                "sepa_mandates.customer_id: expected 16 bytes, got {}",
                m.customer_id.len()
            )
        })?;
        Ok(SepaMandate {
            id: Uuid(id_bytes),
            customer_id: Uuid(customer_id_bytes),
            mandate_ref: m.mandate_ref,
            iban: m.iban,
            bic: m.bic,
            holder_name: m.holder_name,
            signed_at: m.signed_at,
            status: m.status,
        })
    }
}

#[query]
async fn all(ctx: &AppCtx) -> anyhow::Result<Vec<SepaMandate>> {
    let models = entity::Entity::find()
        .filter(entity::Column::TenantId.eq(DEMO_TENANT_ID))
        .all(&ctx.db)
        .await?;
    models.into_iter().map(SepaMandate::try_from).collect()
}
