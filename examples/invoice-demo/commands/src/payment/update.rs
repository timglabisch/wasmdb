use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use sql_engine::storage::Uuid;
use ts_rs::TS;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str, p_uuid};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct UpdatePayment {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "number")]
    pub amount: i64,
    pub paid_at: String,
    pub method: String,
    pub reference: String,
    pub note: String,
}

impl Command for UpdatePayment {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let params = Params::from([
            p_uuid("id", &self.id),
            p_int("amount", self.amount),
            p_str("paid_at", &self.paid_at),
            p_str("method", &self.method),
            p_str("reference", &self.reference),
            p_str("note", &self.note),
        ]);
        execute_sql(db,
            "UPDATE payments SET amount = :amount, paid_at = :paid_at, method = :method, reference = :reference, note = :note WHERE payments.id = :id",
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
    impl ServerCommand for UpdatePayment {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            sqlx::query(
                "UPDATE payments SET amount = ?, paid_at = ?, method = ?, reference = ?, note = ? WHERE payments.id = ?",
            )
                .bind(self.amount)
                .bind(&self.paid_at)
                .bind(&self.method)
                .bind(&self.reference)
                .bind(&self.note)
                .bind(&self.id.0[..])
                .execute(&mut **tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "UPDATE payment {}: {e}",
                    self.id,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
