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
pub struct CreatePayment {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "string")]
    pub invoice_id: Uuid,
    pub amount: i64,
    pub paid_at: String,
    pub method: String,
    pub reference: String,
    pub note: String,
}

impl Command for CreatePayment {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        execute_sql(
            db,
            "INSERT INTO payments (id, invoice_id, amount, paid_at, method, reference, note) \
             VALUES (:id, :invoice_id, :amount, :paid_at, :method, :reference, :note)",
            Params::from([
                p_uuid("id", &self.id),
                p_uuid("invoice_id", &self.invoice_id),
                p_int("amount", self.amount),
                p_str("paid_at", &self.paid_at),
                p_str("method", &self.method),
                p_str("reference", &self.reference),
                p_str("note", &self.note),
            ]),
        )
    }
}

/// Server-authoritative balance check. Two clients' optimistic checks can
/// each pass locally and still together overpay the invoice — re-verifying
/// inside the TiDB transaction resolves the race.
#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sqlx::{MySql, Transaction};
    use sync_server_mysql::ServerCommand;

    #[async_trait]
    impl ServerCommand for CreatePayment {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            if self.amount <= 0 {
                return Err(CommandError::ExecutionFailed(format!(
                    "payment amount must be positive, got {}",
                    self.amount,
                )));
            }

            // `CAST(... AS SIGNED)` pins MySQL's DECIMAL SUM back to i64.
            let remaining: i64 = sqlx::query_scalar(
                "SELECT CAST( \
                   COALESCE((SELECT SUM(quantity*unit_price) FROM positions WHERE invoice_id=?), 0) \
                 - COALESCE((SELECT SUM(amount)              FROM payments  WHERE invoice_id=?), 0) \
                 AS SIGNED)",
            )
            .bind(&self.invoice_id.0[..])
            .bind(&self.invoice_id.0[..])
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "balance lookup for invoice {}: {e}",
                self.invoice_id,
            )))?;

            if self.amount > remaining {
                return Err(CommandError::ExecutionFailed(format!(
                    "overpayment: invoice {} has {} remaining, got {}",
                    self.invoice_id, remaining, self.amount,
                )));
            }

            sqlx::query(
                "INSERT INTO payments (id, invoice_id, amount, paid_at, method, reference, note) \
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&self.id.0[..])
            .bind(&self.invoice_id.0[..])
            .bind(self.amount)
            .bind(&self.paid_at)
            .bind(&self.method)
            .bind(&self.reference)
            .bind(&self.note)
            .execute(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "INSERT payment {}: {e}", self.id,
            )))?;
            Ok(client_zset.clone())
        }
    }
}
