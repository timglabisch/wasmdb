use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct MovePosition {
    pub id: i64,
    pub new_position_nr: i64,
}

impl Command for MovePosition {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let params = Params::from([
            p_int("id", self.id),
            p_int("position_nr", self.new_position_nr),
        ]);
        execute_sql(db,
            "UPDATE positions SET position_nr = :position_nr WHERE positions.id = :id",
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
    impl ServerCommand for MovePosition {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            sqlx::query(
                "UPDATE positions SET position_nr = ? WHERE id = ?"
            )
                .bind(self.new_position_nr)
                .bind(self.id)
                .execute(&mut **tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "UPDATE position id={} position_nr={}: {e}",
                    self.id, self.new_position_nr,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
