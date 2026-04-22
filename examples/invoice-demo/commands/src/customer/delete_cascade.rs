use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use database::Database;
use sql_engine::execute::{Params, ParamValue};
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, read_i64_col};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct DeleteCustomerCascade {
    pub id: i64,
}

impl Command for DeleteCustomerCascade {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let id = self.id;

        let recurring_ids = read_i64_col(db,
            "SELECT id FROM recurring_invoices WHERE customer_id = :cid",
            Params::from([p_int("cid", id)]))?;
        let invoice_ids = read_i64_col(db,
            "SELECT id FROM invoices WHERE customer_id = :cid",
            Params::from([p_int("cid", id)]))?;

        let mut acc = ZSet::new();

        if !recurring_ids.is_empty() {
            let p = Params::from([
                ("rids".into(), ParamValue::IntList(recurring_ids.clone())),
            ]);
            acc.extend(execute_sql(db,
                "DELETE FROM recurring_positions WHERE recurring_id IN (:rids)", p)?);
            let p = Params::from([
                ("rids".into(), ParamValue::IntList(recurring_ids)),
            ]);
            acc.extend(execute_sql(db,
                "DELETE FROM recurring_invoices WHERE id IN (:rids)", p)?);
        }

        if !invoice_ids.is_empty() {
            let p = Params::from([
                ("iids".into(), ParamValue::IntList(invoice_ids.clone())),
            ]);
            acc.extend(execute_sql(db,
                "DELETE FROM payments WHERE invoice_id IN (:iids)", p)?);
            let p = Params::from([
                ("iids".into(), ParamValue::IntList(invoice_ids.clone())),
            ]);
            acc.extend(execute_sql(db,
                "DELETE FROM positions WHERE invoice_id IN (:iids)", p)?);
            let p = Params::from([
                ("iids".into(), ParamValue::IntList(invoice_ids)),
            ]);
            acc.extend(execute_sql(db,
                "DELETE FROM invoices WHERE id IN (:iids)", p)?);
        }

        let p = Params::from([p_int("cid", id)]);
        acc.extend(execute_sql(db,
            "DELETE FROM sepa_mandates WHERE customer_id = :cid", p)?);
        let p = Params::from([p_int("cid", id)]);
        acc.extend(execute_sql(db,
            "DELETE FROM contacts WHERE customer_id = :cid", p)?);
        let p = Params::from([p_int("id", id)]);
        acc.extend(execute_sql(db,
            "DELETE FROM customers WHERE id = :id", p)?);

        Ok(acc)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sqlx::{MySql, Transaction};
    use sync_server_mysql::ServerCommand;

    /// Builds a comma-separated `?,?,?` placeholder list for IN-clauses.
    fn qmarks(n: usize) -> String {
        std::iter::repeat("?").take(n).collect::<Vec<_>>().join(",")
    }

    #[async_trait]
    impl ServerCommand for DeleteCustomerCascade {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let id = self.id;

            let recurring_ids: Vec<i64> = sqlx::query_scalar(
                "SELECT id FROM recurring_invoices WHERE customer_id = ?",
            )
            .bind(id)
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "lookup recurring_invoices for customer {id}: {e}",
            )))?;

            let invoice_ids: Vec<i64> = sqlx::query_scalar(
                "SELECT id FROM invoices WHERE customer_id = ?",
            )
            .bind(id)
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "lookup invoices for customer {id}: {e}",
            )))?;

            if !recurring_ids.is_empty() {
                let sql = format!(
                    "DELETE FROM recurring_positions WHERE recurring_id IN ({})",
                    qmarks(recurring_ids.len()),
                );
                let mut q = sqlx::query(&sql);
                for rid in &recurring_ids { q = q.bind(rid); }
                q.execute(&mut **tx).await.map_err(|e| CommandError::ExecutionFailed(
                    format!("DELETE recurring_positions for customer {id}: {e}"),
                ))?;

                let sql = format!(
                    "DELETE FROM recurring_invoices WHERE id IN ({})",
                    qmarks(recurring_ids.len()),
                );
                let mut q = sqlx::query(&sql);
                for rid in &recurring_ids { q = q.bind(rid); }
                q.execute(&mut **tx).await.map_err(|e| CommandError::ExecutionFailed(
                    format!("DELETE recurring_invoices for customer {id}: {e}"),
                ))?;
            }

            if !invoice_ids.is_empty() {
                let sql = format!(
                    "DELETE FROM payments WHERE invoice_id IN ({})",
                    qmarks(invoice_ids.len()),
                );
                let mut q = sqlx::query(&sql);
                for iid in &invoice_ids { q = q.bind(iid); }
                q.execute(&mut **tx).await.map_err(|e| CommandError::ExecutionFailed(
                    format!("DELETE payments for customer {id}: {e}"),
                ))?;

                let sql = format!(
                    "DELETE FROM positions WHERE invoice_id IN ({})",
                    qmarks(invoice_ids.len()),
                );
                let mut q = sqlx::query(&sql);
                for iid in &invoice_ids { q = q.bind(iid); }
                q.execute(&mut **tx).await.map_err(|e| CommandError::ExecutionFailed(
                    format!("DELETE positions for customer {id}: {e}"),
                ))?;

                let sql = format!(
                    "DELETE FROM invoices WHERE id IN ({})",
                    qmarks(invoice_ids.len()),
                );
                let mut q = sqlx::query(&sql);
                for iid in &invoice_ids { q = q.bind(iid); }
                q.execute(&mut **tx).await.map_err(|e| CommandError::ExecutionFailed(
                    format!("DELETE invoices for customer {id}: {e}"),
                ))?;
            }

            sqlx::query("DELETE FROM sepa_mandates WHERE customer_id = ?")
                .bind(id).execute(&mut **tx).await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE sepa_mandates for customer {id}: {e}",
                )))?;

            sqlx::query("DELETE FROM contacts WHERE customer_id = ?")
                .bind(id).execute(&mut **tx).await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE contacts for customer {id}: {e}",
                )))?;

            sqlx::query("DELETE FROM customers WHERE id = ?")
                .bind(id).execute(&mut **tx).await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE customer {id}: {e}",
                )))?;

            Ok(client_zset.clone())
        }
    }
}
