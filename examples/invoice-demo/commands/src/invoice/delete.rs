use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int};

/// Cascades positions + payments + invoice — all in one atomic ZSet.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct DeleteInvoice {
    pub id: i64,
}

impl Command for DeleteInvoice {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let id = self.id;
        let mut acc = ZSet::new();
        let p = Params::from([p_int("iid", id)]);
        acc.extend(execute_sql(db,
            "DELETE FROM payments WHERE invoice_id = :iid", p)?);
        let p = Params::from([p_int("iid", id)]);
        acc.extend(execute_sql(db,
            "DELETE FROM positions WHERE invoice_id = :iid", p)?);
        let p = Params::from([p_int("id", id)]);
        acc.extend(execute_sql(db,
            "DELETE FROM invoices WHERE id = :id", p)?);
        Ok(acc)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use std::collections::HashMap;
    use async_trait::async_trait;
    use sql_engine::schema::TableSchema;
    use sqlx::{MySql, Transaction};
    use sync_server_mysql::ServerCommand;

    #[async_trait]
    impl ServerCommand for DeleteInvoice {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
            _schemas: &HashMap<String, TableSchema>,
        ) -> Result<ZSet, CommandError> {
            sqlx::query("DELETE FROM payments WHERE invoice_id = ?")
                .bind(self.id)
                .execute(&mut **tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE payments for invoice {}: {e}",
                    self.id,
                )))?;
            sqlx::query("DELETE FROM positions WHERE invoice_id = ?")
                .bind(self.id)
                .execute(&mut **tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE positions for invoice {}: {e}",
                    self.id,
                )))?;
            sqlx::query("DELETE FROM invoices WHERE id = ?")
                .bind(self.id)
                .execute(&mut **tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE invoice {}: {e}",
                    self.id,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
