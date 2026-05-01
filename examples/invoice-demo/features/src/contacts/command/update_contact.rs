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
    use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter};
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
            contact_entity::Entity::update_many()
                .col_expr(contact_entity::Column::Name, self.name.clone().into())
                .col_expr(contact_entity::Column::Email, self.email.clone().into())
                .col_expr(contact_entity::Column::Phone, self.phone.clone().into())
                .col_expr(contact_entity::Column::Role, self.role.clone().into())
                .col_expr(contact_entity::Column::IsPrimary, self.is_primary.into())
                .filter(contact_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(contact_entity::Column::Id.eq(self.id.0.to_vec()))
                .exec(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "UPDATE contact {}: {e}", self.id,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
