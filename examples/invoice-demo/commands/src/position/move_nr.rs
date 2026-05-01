use sql_engine::storage::Uuid;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use rpc_command::rpc_command;
use crate::helpers::{execute_sql, p_int, p_uuid, DEMO_TENANT_ID};

#[rpc_command]
pub struct MovePosition {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "number")]
    pub new_position_nr: i64,
}

impl Command for MovePosition {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let params = Params::from([
            p_uuid("id", &self.id),
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
                "UPDATE positions SET position_nr = ? WHERE tenant_id = ? AND id = ?"
            )
                .bind(self.new_position_nr)
                .bind(DEMO_TENANT_ID)
                .bind(&self.id.0[..])
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
