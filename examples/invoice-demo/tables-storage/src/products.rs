use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use sql_engine::storage::Uuid;
use tables_storage::{query, row};

use crate::{AppCtx, DEMO_TENANT_ID};

#[row(table = "products")]
#[export(name = "", groups = ["all"])]
#[export(name = "WithoutPk", groups = ["non_pk"])]
pub struct Product {
    #[pk]
    pub id: Uuid,
    pub sku: String,
    pub name: String,
    pub description: String,
    pub unit: String,
    pub unit_price: i64,
    pub tax_rate: i64,
    pub cost_price: i64,
    pub active: i64,
}

mod entity {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(schema_name = "invoice_demo", table_name = "products")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false, column_type = "Binary(16)")]
        pub id: Vec<u8>,
        pub tenant_id: i64,
        pub sku: String,
        pub name: String,
        pub description: String,
        pub unit: String,
        pub unit_price: i64,
        pub tax_rate: i64,
        pub cost_price: i64,
        pub active: i64,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

#[query]
async fn all(ctx: &AppCtx) -> anyhow::Result<Vec<Product>> {
    let models = entity::Entity::find()
        .filter(entity::Column::TenantId.eq(DEMO_TENANT_ID))
        .all(&ctx.db)
        .await?;

    models
        .into_iter()
        .map(|m| {
            let id_bytes: [u8; 16] = m.id.as_slice().try_into().map_err(|_| {
                anyhow::anyhow!(
                    "products.id: expected 16 bytes, got {}",
                    m.id.len()
                )
            })?;
            Ok(Product {
                id: Uuid(id_bytes),
                sku: m.sku,
                name: m.name,
                description: m.description,
                unit: m.unit,
                unit_price: m.unit_price,
                tax_rate: m.tax_rate,
                cost_price: m.cost_price,
                active: m.active,
            })
        })
        .collect()
}
