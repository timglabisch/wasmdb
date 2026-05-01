use sql_engine::storage::Uuid;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use rpc_command::rpc_command;
use crate::command_helpers::{execute_sql, p_str, p_uuid};

#[rpc_command]
pub struct LogActivity {
    #[ts(type = "string")]
    #[client_default = "nextId()"]
    pub id: Uuid,
    #[client_default = "new Date().toISOString()"]
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
    use sea_orm::{DatabaseTransaction, EntityTrait, Set};
    use sync_server_mysql::ServerCommand;

    use crate::activity_log::activity_log_server::entity as activity_entity;
    use crate::shared::DEMO_TENANT_ID;

    #[async_trait]
    impl ServerCommand for LogActivity {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let am = activity_entity::ActiveModel {
                tenant_id: Set(DEMO_TENANT_ID),
                id: Set(self.id.0.to_vec()),
                timestamp: Set(self.timestamp.clone()),
                entity_type: Set(self.entity_type.clone()),
                entity_id: Set(self.entity_id.0.to_vec()),
                action: Set(self.action.clone()),
                actor: Set(self.actor.clone()),
                detail: Set(self.detail.clone()),
            };
            activity_entity::Entity::insert(am)
                .on_conflict_do_nothing()
                .exec_without_returning(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "INSERT activity {}: {e}", self.id,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
