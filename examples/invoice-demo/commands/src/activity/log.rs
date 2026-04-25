use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use sql_engine::storage::Uuid;
use ts_rs::TS;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_str, p_uuid, DEMO_TENANT_ID};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct LogActivity {
    #[ts(type = "string")]
    pub id: Uuid,
    pub timestamp: String,
    pub entity_type: String,
    #[ts(type = "string")]
    pub entity_id: Uuid,
    pub action: String,
    pub actor: String,
    pub detail: String,
}

impl Command for LogActivity {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let params = Params::from([
            p_uuid("id", &self.id),
            p_str("timestamp", &self.timestamp),
            p_str("entity_type", &self.entity_type),
            p_uuid("entity_id", &self.entity_id),
            p_str("action", &self.action),
            p_str("actor", &self.actor),
            p_str("detail", &self.detail),
        ]);
        execute_sql(db,
            "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
             VALUES (:id, :timestamp, :entity_type, :entity_id, :action, :actor, :detail)",
            params)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sqlx::{MySql, Transaction};
    use sync_server_mysql::ServerCommand;

    #[async_trait]
    impl ServerCommand for LogActivity {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            sqlx::query(
                "INSERT INTO activity_log (tenant_id, id, timestamp, entity_type, entity_id, action, actor, detail) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(DEMO_TENANT_ID)
            .bind(&self.id.0[..])
            .bind(&self.timestamp)
            .bind(&self.entity_type)
            .bind(&self.entity_id.0[..])
            .bind(&self.action)
            .bind(&self.actor)
            .bind(&self.detail)
            .execute(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "INSERT activity {}: {e}",
                self.id,
            )))?;
            Ok(client_zset.clone())
        }
    }
}
