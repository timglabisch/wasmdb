use sql_engine::storage::Uuid;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use rpc_command::rpc_command;
use crate::command_helpers::{execute_sql, p_int, p_str, p_uuid};

#[rpc_command]
pub struct UpdateContact {
    #[ts(type = "string")]
    pub id: Uuid,
    pub name: String,
    pub email: String,
    pub phone: String,
    pub role: String,
    #[ts(type = "number")]
    pub is_primary: i64,
}

impl Command for UpdateContact {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let params = Params::from([
            p_uuid("id", &self.id),
            p_str("name", &self.name),
            p_str("email", &self.email),
            p_str("phone", &self.phone),
            p_str("role", &self.role),
            p_int("is_primary", self.is_primary),
        ]);
        execute_sql(db,
            "UPDATE contacts SET name = :name, email = :email, phone = :phone, role = :role, is_primary = :is_primary WHERE contacts.id = :id",
            params)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter, Set};
    use sync_server_mysql::ServerCommand;

    use crate::contacts::contact_server::entity as contact_entity;
    use crate::shared::DEMO_TENANT_ID;

    #[async_trait]
    impl ServerCommand for UpdateContact {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let model = contact_entity::Entity::find()
                .filter(contact_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(contact_entity::Column::Id.eq(self.id.0.to_vec()))
                .one(tx).await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "load contact {}: {e}", self.id,
                )))?
                .ok_or_else(|| CommandError::ExecutionFailed(format!(
                    "contact {} not found", self.id,
                )))?;

            let mut am: contact_entity::ActiveModel = model.into();
            am.name = Set(self.name.clone());
            am.email = Set(self.email.clone());
            am.phone = Set(self.phone.clone());
            am.role = Set(self.role.clone());
            am.is_primary = Set(self.is_primary);
            am.update(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                "UPDATE contact {}: {e}", self.id,
            )))?;
            Ok(client_zset.clone())
        }
    }
}
