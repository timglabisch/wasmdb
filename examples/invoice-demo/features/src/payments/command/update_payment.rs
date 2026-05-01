use sql_engine::storage::Uuid;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use rpc_command::rpc_command;
use crate::command_helpers::{execute_sql, p_int, p_str, p_uuid};

#[rpc_command]
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
    use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter, Set};
    use sync_server_mysql::ServerCommand;

    use crate::payments::payment_server::entity as payment_entity;
    use crate::shared::DEMO_TENANT_ID;

    #[async_trait]
    impl ServerCommand for UpdatePayment {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let model = payment_entity::Entity::find()
                .filter(payment_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(payment_entity::Column::Id.eq(self.id.0.to_vec()))
                .one(tx).await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "load payment {}: {e}", self.id,
                )))?
                .ok_or_else(|| CommandError::ExecutionFailed(format!(
                    "payment {} not found", self.id,
                )))?;

            let mut am: payment_entity::ActiveModel = model.into();
            am.amount = Set(self.amount);
            am.paid_at = Set(self.paid_at.clone());
            am.method = Set(self.method.clone());
            am.reference = Set(self.reference.clone());
            am.note = Set(self.note.clone());
            am.update(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                "UPDATE payment {}: {e}", self.id,
            )))?;
            Ok(client_zset.clone())
        }
    }
}
