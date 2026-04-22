use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct DeleteSepaMandate {
    pub id: i64,
}

impl Command for DeleteSepaMandate {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let params = Params::from([p_int("id", self.id)]);
        execute_sql(db, "DELETE FROM sepa_mandates WHERE sepa_mandates.id = :id", params)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sqlx::{MySql, Transaction};
    use sync_server_mysql::ServerCommand;

    #[async_trait]
    impl ServerCommand for DeleteSepaMandate {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            sqlx::query("DELETE FROM sepa_mandates WHERE id = ?")
                .bind(self.id)
                .execute(&mut **tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE sepa_mandate id={}: {e}",
                    self.id,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
