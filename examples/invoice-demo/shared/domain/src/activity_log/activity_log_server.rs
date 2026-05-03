//! Server-side: SeaORM Entity for `activity_log` + Fetcher.
//! The Entity is `pub` so other features can insert audit rows by
//! constructing an `ActiveModel` directly — see `insert_activity`.

#![cfg(feature = "server")]

use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseTransaction, EntityTrait,
    QueryFilter, Statement, Value,
};
use sql_engine::storage::Uuid;
use sync::command::CommandError;
use tables_storage::query;

use super::activity_log_client::ActivityLogEntry;
use crate::shared::DEMO_TENANT_ID;
use crate::AppCtx;

pub mod entity {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(schema_name = "invoice_demo", table_name = "activity_log")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false, column_type = "Binary(16)")]
        pub id: Vec<u8>,
        pub tenant_id: i64,
        pub timestamp: String,
        pub entity_type: String,
        #[sea_orm(column_type = "Binary(16)")]
        pub entity_id: Vec<u8>,
        pub action: String,
        pub actor: String,
        pub detail: String,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

impl TryFrom<entity::Model> for ActivityLogEntry {
    type Error = anyhow::Error;
    fn try_from(m: entity::Model) -> Result<Self, Self::Error> {
        let id_bytes: [u8; 16] = m.id.as_slice().try_into().map_err(|_| {
            anyhow::anyhow!("activity_log.id: expected 16 bytes, got {}", m.id.len())
        })?;
        let entity_id_bytes: [u8; 16] = m.entity_id.as_slice().try_into().map_err(|_| {
            anyhow::anyhow!(
                "activity_log.entity_id: expected 16 bytes, got {}",
                m.entity_id.len()
            )
        })?;
        Ok(ActivityLogEntry {
            id: Uuid(id_bytes),
            timestamp: m.timestamp,
            entity_type: m.entity_type,
            entity_id: Uuid(entity_id_bytes),
            action: m.action,
            actor: m.actor,
            detail: m.detail,
        })
    }
}

#[query]
async fn all(ctx: &AppCtx) -> anyhow::Result<Vec<ActivityLogEntry>> {
    let models = entity::Entity::find()
        .filter(entity::Column::TenantId.eq(DEMO_TENANT_ID))
        .all(&ctx.db)
        .await?;
    models.into_iter().map(ActivityLogEntry::try_from).collect()
}

/// Insert an audit row. Idempotent on `(tenant_id, id)` via
/// `ON DUPLICATE KEY UPDATE id = id` — re-applying the parent command
/// after a partial-failure retry is safe.
pub async fn insert_activity(
    tx: &DatabaseTransaction,
    activity_id: &Uuid,
    timestamp: &str,
    entity_type: &str,
    entity_id: &Uuid,
    action: &str,
    detail: &str,
) -> Result<(), CommandError> {
    let stmt = Statement::from_sql_and_values(
        DatabaseBackend::MySql,
        "INSERT INTO invoice_demo.activity_log \
         (tenant_id, id, timestamp, entity_type, entity_id, action, actor, detail) \
         VALUES (?, ?, ?, ?, ?, ?, 'demo', ?) \
         ON DUPLICATE KEY UPDATE id = id",
        [
            Value::from(DEMO_TENANT_ID),
            Value::from(activity_id.0.to_vec()),
            Value::from(timestamp.to_string()),
            Value::from(entity_type.to_string()),
            Value::from(entity_id.0.to_vec()),
            Value::from(action.to_string()),
            Value::from(detail.to_string()),
        ],
    );
    tx.execute_raw(stmt).await.map_err(|e| {
        CommandError::ExecutionFailed(format!("INSERT activity {activity_id}: {e}"))
    })?;
    Ok(())
}
