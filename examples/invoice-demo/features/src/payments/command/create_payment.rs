use sql_engine::storage::Uuid;
use database::Database;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use rpc_command::rpc_command;
use crate::command_helpers::SqlStmtExt;
use crate::shared::DEMO_TENANT_ID;

#[rpc_command]
pub struct CreatePayment {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "string")]
    pub invoice_id: Uuid,
    #[ts(type = "number")]
    pub amount: i64,
    pub paid_at: String,
    pub method: String,
    pub reference: String,
    pub note: String,
}

impl Command for CreatePayment {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        let Self { id, invoice_id, amount, paid_at, method, reference, note } = self;
        sql!(
            "INSERT INTO payments (id, invoice_id, amount, paid_at, method, reference, note) \
             VALUES ({id}, {invoice_id}, {amount}, {paid_at}, {method}, {reference}, {note})"
        )
        .execute(db)
    }
}

/// Server-authoritative balance check. Two clients' optimistic checks can
/// each pass locally and still together overpay the invoice — re-verifying
/// inside the TiDB transaction resolves the race.
#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sea_orm::{
        ConnectionTrait, DatabaseBackend, DatabaseTransaction, EntityTrait, Set, Statement, Value,
    };
    use sync_server_mysql::ServerCommand;

    use crate::payments::payment_server::entity as payment_entity;

    #[async_trait]
    impl ServerCommand for CreatePayment {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            if self.amount <= 0 {
                return Err(CommandError::ExecutionFailed(format!(
                    "payment amount must be positive, got {}",
                    self.amount,
                )));
            }

            // `CAST(... AS SIGNED)` pins MySQL's DECIMAL SUM back to i64.
            let stmt = Statement::from_sql_and_values(
                DatabaseBackend::MySql,
                "SELECT CAST( \
                   COALESCE((SELECT SUM(quantity*unit_price) FROM invoice_demo.positions WHERE tenant_id=? AND invoice_id=?), 0) \
                 - COALESCE((SELECT SUM(amount)              FROM invoice_demo.payments  WHERE tenant_id=? AND invoice_id=?), 0) \
                 AS SIGNED)",
                [
                    Value::from(DEMO_TENANT_ID),
                    Value::from(self.invoice_id.0.to_vec()),
                    Value::from(DEMO_TENANT_ID),
                    Value::from(self.invoice_id.0.to_vec()),
                ],
            );
            let row = tx.query_one_raw(stmt).await.map_err(|e| {
                CommandError::ExecutionFailed(format!(
                    "balance lookup for invoice {}: {e}",
                    self.invoice_id,
                ))
            })?;
            let row = row.ok_or_else(|| {
                CommandError::ExecutionFailed(format!(
                    "balance lookup for invoice {}: no row returned",
                    self.invoice_id,
                ))
            })?;
            let remaining: i64 = row.try_get_by_index(0).map_err(|e| {
                CommandError::ExecutionFailed(format!(
                    "balance lookup for invoice {}: {e}",
                    self.invoice_id,
                ))
            })?;

            if self.amount > remaining {
                return Err(CommandError::ExecutionFailed(format!(
                    "overpayment: invoice {} has {} remaining, got {}",
                    self.invoice_id, remaining, self.amount,
                )));
            }

            let am = payment_entity::ActiveModel {
                tenant_id: Set(DEMO_TENANT_ID),
                id: Set(self.id.0.to_vec()),
                invoice_id: Set(self.invoice_id.0.to_vec()),
                amount: Set(self.amount),
                paid_at: Set(self.paid_at.clone()),
                method: Set(self.method.clone()),
                reference: Set(self.reference.clone()),
                note: Set(self.note.clone()),
            };
            payment_entity::Entity::insert(am)
                .on_conflict_do_nothing()
                .exec_without_returning(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "INSERT payment {}: {e}", self.id,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
