//! Server-side: SeaORM Entity for the `contacts` table + `From<Model>`
//! adapter to the client DTO + `#[query]`-fns. Owns the SQL-side schema.

#![cfg(feature = "server")]

use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
use sql_engine::storage::Uuid;
use tables_storage::query;

use crate::shared::DEMO_TENANT_ID;
use crate::AppCtx;
use super::contact_client::Contact;

pub mod entity {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(schema_name = "invoice_demo", table_name = "contacts")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false, column_type = "Binary(16)")]
        pub id: Vec<u8>,
        pub tenant_id: i64,
        #[sea_orm(column_type = "Binary(16)")]
        pub customer_id: Vec<u8>,
        pub name: String,
        pub email: String,
        pub phone: String,
        pub role: String,
        pub is_primary: i64,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

impl TryFrom<entity::Model> for Contact {
    type Error = anyhow::Error;
    fn try_from(m: entity::Model) -> Result<Self, Self::Error> {
        let id_bytes: [u8; 16] = m.id.as_slice().try_into().map_err(|_| {
            anyhow::anyhow!("contacts.id: expected 16 bytes, got {}", m.id.len())
        })?;
        let customer_id_bytes: [u8; 16] = m.customer_id.as_slice().try_into().map_err(|_| {
            anyhow::anyhow!(
                "contacts.customer_id: expected 16 bytes, got {}",
                m.customer_id.len()
            )
        })?;
        Ok(Contact {
            id: Uuid(id_bytes),
            customer_id: Uuid(customer_id_bytes),
            name: m.name,
            email: m.email,
            phone: m.phone,
            role: m.role,
            is_primary: m.is_primary,
        })
    }
}

#[query]
async fn all(ctx: &AppCtx) -> anyhow::Result<Vec<Contact>> {
    let models = entity::Entity::find()
        .filter(entity::Column::TenantId.eq(DEMO_TENANT_ID))
        .all(&ctx.db)
        .await?;
    models.into_iter().map(Contact::try_from).collect()
}
