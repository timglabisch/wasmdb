use sql_engine::storage::Uuid;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use rpc_command::rpc_command;
use crate::helpers::{execute_sql, p_uuid, DEMO_TENANT_ID};

#[rpc_command]
pub struct DeleteContact {
    #[ts(type = "string")]
    pub id: Uuid,
}

impl Command for DeleteContact {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let params = Params::from([p_uuid("id", &self.id)]);
        execute_sql(db, "DELETE FROM contacts WHERE contacts.id = :id", params)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sqlx::{MySql, Transaction};
    use sync_server_mysql::ServerCommand;

    #[async_trait]
    impl ServerCommand for DeleteContact {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            sqlx::query("DELETE FROM contacts WHERE tenant_id = ? AND id = ?")
                .bind(DEMO_TENANT_ID)
                .bind(&self.id.0[..])
                .execute(&mut **tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE contact {}: {e}",
                    self.id,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
