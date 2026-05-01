use sql_engine::storage::Uuid;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use rpc_command::rpc_command;
use crate::helpers::{execute_sql, p_int, p_str, p_uuid, DEMO_TENANT_ID};

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
        let params = Params::from([
            p_uuid("id", &self.id),
            p_uuid("customer_id", &self.customer_id),
            p_str("name", &self.name),
            p_str("email", &self.email),
            p_str("phone", &self.phone),
            p_str("role", &self.role),
            p_int("is_primary", self.is_primary),
        ]);
        let mut acc = execute_sql(db,
            "INSERT INTO contacts (id, customer_id, name, email, phone, role, is_primary) \
             VALUES (:id, :customer_id, :name, :email, :phone, :role, :is_primary)",
            params)?;

        let detail = detail_for(&self.name);
        // entity_type='customer', entity_id=customer_id — preserves original semantics
        acc.extend(execute_sql(
            db,
            "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
             VALUES (:aid, :ts, 'customer', :customer_id, 'contact_create', 'demo', :detail)",
            Params::from([
                p_uuid("aid", &self.activity_id),
                p_str("ts", &self.timestamp),
                p_uuid("customer_id", &self.customer_id),
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
    impl ServerCommand for CreateContact {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            sqlx::query(
                "INSERT INTO contacts (tenant_id, id, customer_id, name, email, phone, role, is_primary) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
                 ON DUPLICATE KEY UPDATE id = id",
            )
            .bind(DEMO_TENANT_ID)
            .bind(&self.id.0[..])
            .bind(&self.customer_id.0[..])
            .bind(&self.name)
            .bind(&self.email)
            .bind(&self.phone)
            .bind(&self.role)
            .bind(self.is_primary)
            .execute(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "INSERT contact {}: {e}",
                self.id,
            )))?;

            let detail = detail_for(&self.name);
            // entity_type='customer', entity_id=customer_id — preserves original semantics
            sqlx::query(
                "INSERT INTO activity_log (tenant_id, id, timestamp, entity_type, entity_id, action, actor, detail) \
                 VALUES (?, ?, ?, 'customer', ?, 'contact_create', 'demo', ?) \
                 ON DUPLICATE KEY UPDATE id = id",
            )
            .bind(DEMO_TENANT_ID)
            .bind(&self.activity_id.0[..])
            .bind(&self.timestamp)
            .bind(&self.customer_id.0[..])
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
