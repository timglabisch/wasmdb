use borsh::{BorshDeserialize, BorshSerialize};
use database::Database;
use serde::{Deserialize, Serialize};
use sql_engine::execute::Params;
use sql_engine::storage::Uuid;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use ts_rs::TS;

use crate::helpers::{execute_sql, p_int, p_str, p_uuid, read_str_col, DEMO_TENANT_ID};

/// Intent-Command: activate or deactivate a product.
///
/// Replaces the old `updateProduct({...,active}) + logActivity(...)` pair.
/// The activity row is produced inside `execute_optimistic` / `execute_server`
/// from the product name — callers no longer compose the audit-log entry
/// themselves. `activity_id` + `timestamp` are passed in by the client wrapper
/// so client-optimistic and server-authoritative inserts share the same primary
/// key (idempotent re-apply).
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct SetProductActive {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "number")]
    pub active: i64,
    #[ts(type = "string")]
    pub activity_id: Uuid,
    pub timestamp: String,
}

fn detail_for(name: &str, active: i64) -> String {
    if active == 1 {
        format!("Produkt \"{name}\" aktiviert")
    } else {
        format!("Produkt \"{name}\" deaktiviert")
    }
}

fn action_for(active: i64) -> &'static str {
    if active == 1 { "activate" } else { "deactivate" }
}

impl Command for SetProductActive {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        let mut acc = execute_sql(
            db,
            "UPDATE products SET active = :active WHERE products.id = :id",
            Params::from([
                p_int("active", self.active),
                p_uuid("id", &self.id),
            ]),
        )?;

        let names = read_str_col(
            db,
            "SELECT products.name FROM products WHERE products.id = :id",
            Params::from([p_uuid("id", &self.id)]),
        )?;
        let name = names.into_iter().next().unwrap_or_default();
        let detail = detail_for(&name, self.active);
        let action = action_for(self.active);

        acc.extend(execute_sql(
            db,
            "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
             VALUES (:aid, :ts, 'product', :id, :action, 'demo', :detail)",
            Params::from([
                p_uuid("aid", &self.activity_id),
                p_str("ts", &self.timestamp),
                p_uuid("id", &self.id),
                p_str("action", action),
                p_str("detail", &detail),
            ]),
        )?);

        Ok(acc)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sqlx::{MySql, Transaction};
    use sync_server_mysql::ServerCommand;

    #[async_trait]
    impl ServerCommand for SetProductActive {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            sqlx::query(
                "UPDATE products SET active = ? \
                 WHERE tenant_id = ? AND id = ?",
            )
            .bind(self.active)
            .bind(DEMO_TENANT_ID)
            .bind(&self.id.0[..])
            .execute(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "UPDATE product {} -> active={}: {e}", self.id, self.active,
            )))?;

            let name: String = sqlx::query_scalar(
                "SELECT name FROM products WHERE tenant_id = ? AND id = ?",
            )
            .bind(DEMO_TENANT_ID)
            .bind(&self.id.0[..])
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "lookup name for product {}: {e}", self.id,
            )))?;
            let detail = detail_for(&name, self.active);
            let action = action_for(self.active);

            sqlx::query(
                "INSERT INTO activity_log (tenant_id, id, timestamp, entity_type, entity_id, action, actor, detail) \
                 VALUES (?, ?, ?, 'product', ?, ?, 'demo', ?) \
                 ON DUPLICATE KEY UPDATE id = id",
            )
            .bind(DEMO_TENANT_ID)
            .bind(&self.activity_id.0[..])
            .bind(&self.timestamp)
            .bind(&self.id.0[..])
            .bind(action)
            .bind(&detail)
            .execute(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "INSERT activity {}: {e}", self.activity_id,
            )))?;

            Ok(client_zset.clone())
        }
    }
}
