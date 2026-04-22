use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int};

/// Cascades recurring_positions + recurring_invoice atomically.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct DeleteRecurring {
    pub id: i64,
}

impl Command for DeleteRecurring {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let id = self.id;
        let mut acc = ZSet::new();
        let p = Params::from([p_int("rid", id)]);
        acc.extend(execute_sql(db,
            "DELETE FROM recurring_positions WHERE recurring_id = :rid", p)?);
        let p = Params::from([p_int("id", id)]);
        acc.extend(execute_sql(db,
            "DELETE FROM recurring_invoices WHERE id = :id", p)?);
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
    impl ServerCommand for DeleteRecurring {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            sqlx::query("DELETE FROM recurring_positions WHERE recurring_id = ?")
                .bind(self.id)
                .execute(&mut **tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE recurring_positions for recurring_id {}: {e}",
                    self.id,
                )))?;
            sqlx::query("DELETE FROM recurring_invoices WHERE id = ?")
                .bind(self.id)
                .execute(&mut **tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE recurring_invoice {}: {e}",
                    self.id,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
