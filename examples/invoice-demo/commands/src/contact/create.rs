use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct CreateContact {
    pub id: i64,
    pub customer_id: i64,
    pub name: String,
    pub email: String,
    pub phone: String,
    pub role: String,
    pub is_primary: i64,
}

impl Command for CreateContact {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let params = Params::from([
            p_int("id", self.id),
            p_int("customer_id", self.customer_id),
            p_str("name", &self.name),
            p_str("email", &self.email),
            p_str("phone", &self.phone),
            p_str("role", &self.role),
            p_int("is_primary", self.is_primary),
        ]);
        execute_sql(db,
            "INSERT INTO contacts (id, customer_id, name, email, phone, role, is_primary) \
             VALUES (:id, :customer_id, :name, :email, :phone, :role, :is_primary)",
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
    impl ServerCommand for CreateContact {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            sqlx::query(
                "INSERT INTO contacts (id, customer_id, name, email, phone, role, is_primary) \
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(self.id)
            .bind(self.customer_id)
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
            Ok(client_zset.clone())
        }
    }
}
