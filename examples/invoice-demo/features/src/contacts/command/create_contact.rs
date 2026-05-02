use sql_engine::storage::Uuid;
use database::Database;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use rpc_command::rpc_command;
use crate::command_helpers::execute_stmt;
use crate::shared::DEMO_TENANT_ID;

#[rpc_command]
pub struct CreateContact {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "string")]
    pub customer_id: Uuid,
    pub name: String,
    pub email: String,
    pub phone: String,
    pub role: String,
    #[ts(type = "number")]
    pub is_primary: i64,
    #[ts(type = "string")]
    #[client_default = "nextId()"]
    pub activity_id: Uuid,
    #[client_default = "new Date().toISOString()"]
    pub timestamp: String,
}

fn detail_for(name: &str) -> String {
    format!("Ansprechpartner \"{name}\" hinzugefügt")
}

impl Command for CreateContact {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let detail = detail_for(&self.name);
        let mut acc = execute_stmt(
            db,
            sql!(
                "INSERT INTO contacts (id, customer_id, name, email, phone, role, is_primary) \
                 VALUES ({self.id}, {self.customer_id}, {self.name}, {self.email}, {self.phone}, {self.role}, {self.is_primary})"
            ),
        )?;
        // entity_type='customer', entity_id=customer_id — preserves original semantics
        acc.extend(execute_stmt(
            db,
            sql!(
                "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
                 VALUES ({self.activity_id}, {self.timestamp}, 'customer', {self.customer_id}, 'contact_create', 'demo', {detail})"
            ),
        )?);
        Ok(acc)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sea_orm::{DatabaseTransaction, EntityTrait, Set};
    use sync_server_mysql::ServerCommand;

    use crate::activity_log::activity_log_server::insert_activity;
    use crate::contacts::contact_server::entity as contact_entity;

    #[async_trait]
    impl ServerCommand for CreateContact {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let am = contact_entity::ActiveModel {
                tenant_id: Set(DEMO_TENANT_ID),
                id: Set(self.id.0.to_vec()),
                customer_id: Set(self.customer_id.0.to_vec()),
                name: Set(self.name.clone()),
                email: Set(self.email.clone()),
                phone: Set(self.phone.clone()),
                role: Set(self.role.clone()),
                is_primary: Set(self.is_primary),
            };
            contact_entity::Entity::insert(am)
                .on_conflict_do_nothing()
                .exec_without_returning(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "INSERT contact {}: {e}", self.id,
                )))?;

            let detail = detail_for(&self.name);
            // entity_type='customer', entity_id=customer_id — preserves original semantics
            insert_activity(
                tx,
                &self.activity_id,
                &self.timestamp,
                "customer",
                &self.customer_id,
                "contact_create",
                &detail,
            )
            .await?;

            Ok(client_zset.clone())
        }
    }
}
