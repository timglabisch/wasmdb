use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use sql_engine::storage::Uuid;
use ts_rs::TS;
use database::Database;
use sql_engine::execute::{Params, ParamValue};
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_uuid, read_uuid_col, DEMO_TENANT_ID};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct DeleteCustomerCascade {
    #[ts(type = "string")]
    pub id: Uuid,
}

impl Command for DeleteCustomerCascade {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let id = self.id;

        let recurring_ids = read_uuid_col(db,
            "SELECT id FROM recurring_invoices WHERE customer_id = :cid",
            Params::from([p_uuid("cid", &id)]))?;
        let invoice_ids = read_uuid_col(db,
            "SELECT id FROM invoices WHERE customer_id = :cid",
            Params::from([p_uuid("cid", &id)]))?;

        let mut acc = ZSet::new();

        if !recurring_ids.is_empty() {
            let bytes: Vec<[u8; 16]> = recurring_ids.iter().map(|u| u.0).collect();
            let p = Params::from([
                ("rids".into(), ParamValue::UuidList(bytes.clone())),
            ]);
            acc.extend(execute_sql(db,
                "DELETE FROM recurring_positions WHERE recurring_id IN (:rids)", p)?);
            let p = Params::from([
                ("rids".into(), ParamValue::UuidList(bytes)),
            ]);
            acc.extend(execute_sql(db,
                "DELETE FROM recurring_invoices WHERE id IN (:rids)", p)?);
        }

        if !invoice_ids.is_empty() {
            let bytes: Vec<[u8; 16]> = invoice_ids.iter().map(|u| u.0).collect();
            let p = Params::from([
                ("iids".into(), ParamValue::UuidList(bytes.clone())),
            ]);
            acc.extend(execute_sql(db,
                "DELETE FROM payments WHERE invoice_id IN (:iids)", p)?);
            let p = Params::from([
                ("iids".into(), ParamValue::UuidList(bytes.clone())),
            ]);
            acc.extend(execute_sql(db,
                "DELETE FROM positions WHERE invoice_id IN (:iids)", p)?);
            let p = Params::from([
                ("iids".into(), ParamValue::UuidList(bytes)),
            ]);
            acc.extend(execute_sql(db,
                "DELETE FROM invoices WHERE id IN (:iids)", p)?);
        }

        let p = Params::from([p_uuid("cid", &id)]);
        acc.extend(execute_sql(db,
            "DELETE FROM sepa_mandates WHERE customer_id = :cid", p)?);
        let p = Params::from([p_uuid("cid", &id)]);
        acc.extend(execute_sql(db,
            "DELETE FROM contacts WHERE customer_id = :cid", p)?);
        let p = Params::from([p_uuid("id", &id)]);
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

            let recurring_id_rows: Vec<Vec<u8>> = sqlx::query_scalar(
                "SELECT id FROM recurring_invoices WHERE tenant_id = ? AND customer_id = ?",
            )
            .bind(DEMO_TENANT_ID)
            .bind(&id.0[..])
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "lookup recurring_invoices for customer {id}: {e}",
            )))?;

            let invoice_id_rows: Vec<Vec<u8>> = sqlx::query_scalar(
                "SELECT id FROM invoices WHERE tenant_id = ? AND customer_id = ?",
            )
            .bind(DEMO_TENANT_ID)
            .bind(&id.0[..])
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| CommandError::ExecutionFailed(format!(
                "lookup invoices for customer {id}: {e}",
            )))?;

            if !recurring_id_rows.is_empty() {
                let sql = format!(
                    "DELETE FROM recurring_positions WHERE tenant_id = ? AND recurring_id IN ({})",
                    qmarks(recurring_id_rows.len()),
                );
                let mut q = sqlx::query(&sql).bind(DEMO_TENANT_ID);
                for rid in &recurring_id_rows { q = q.bind(rid.as_slice()); }
                q.execute(&mut **tx).await.map_err(|e| CommandError::ExecutionFailed(
                    format!("DELETE recurring_positions for customer {id}: {e}"),
                ))?;

                let sql = format!(
                    "DELETE FROM recurring_invoices WHERE tenant_id = ? AND id IN ({})",
                    qmarks(recurring_id_rows.len()),
                );
                let mut q = sqlx::query(&sql).bind(DEMO_TENANT_ID);
                for rid in &recurring_id_rows { q = q.bind(rid.as_slice()); }
                q.execute(&mut **tx).await.map_err(|e| CommandError::ExecutionFailed(
                    format!("DELETE recurring_invoices for customer {id}: {e}"),
                ))?;
            }

            if !invoice_id_rows.is_empty() {
                let sql = format!(
                    "DELETE FROM payments WHERE tenant_id = ? AND invoice_id IN ({})",
                    qmarks(invoice_id_rows.len()),
                );
                let mut q = sqlx::query(&sql).bind(DEMO_TENANT_ID);
                for iid in &invoice_id_rows { q = q.bind(iid.as_slice()); }
                q.execute(&mut **tx).await.map_err(|e| CommandError::ExecutionFailed(
                    format!("DELETE payments for customer {id}: {e}"),
                ))?;

                let sql = format!(
                    "DELETE FROM positions WHERE tenant_id = ? AND invoice_id IN ({})",
                    qmarks(invoice_id_rows.len()),
                );
                let mut q = sqlx::query(&sql).bind(DEMO_TENANT_ID);
                for iid in &invoice_id_rows { q = q.bind(iid.as_slice()); }
                q.execute(&mut **tx).await.map_err(|e| CommandError::ExecutionFailed(
                    format!("DELETE positions for customer {id}: {e}"),
                ))?;

                let sql = format!(
                    "DELETE FROM invoices WHERE tenant_id = ? AND id IN ({})",
                    qmarks(invoice_id_rows.len()),
                );
                let mut q = sqlx::query(&sql).bind(DEMO_TENANT_ID);
                for iid in &invoice_id_rows { q = q.bind(iid.as_slice()); }
                q.execute(&mut **tx).await.map_err(|e| CommandError::ExecutionFailed(
                    format!("DELETE invoices for customer {id}: {e}"),
                ))?;
            }

            sqlx::query("DELETE FROM sepa_mandates WHERE tenant_id = ? AND customer_id = ?")
                .bind(DEMO_TENANT_ID)
                .bind(&id.0[..]).execute(&mut **tx).await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE sepa_mandates for customer {id}: {e}",
                )))?;

            sqlx::query("DELETE FROM contacts WHERE tenant_id = ? AND customer_id = ?")
                .bind(DEMO_TENANT_ID)
                .bind(&id.0[..]).execute(&mut **tx).await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE contacts for customer {id}: {e}",
                )))?;

            sqlx::query("DELETE FROM customers WHERE tenant_id = ? AND id = ?")
                .bind(DEMO_TENANT_ID)
                .bind(&id.0[..]).execute(&mut **tx).await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "DELETE customer {id}: {e}",
                )))?;

            Ok(client_zset.clone())
        }
    }
}
