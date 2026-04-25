use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use sql_engine::storage::Uuid;
use ts_rs::TS;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_str, p_uuid, DEMO_TENANT_ID};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct CreateSepaMandate {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "string")]
    pub customer_id: Uuid,
    pub mandate_ref: String,
    pub iban: String,
    pub bic: String,
    pub holder_name: String,
    pub signed_at: String,
}

impl Command for CreateSepaMandate {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let params = Params::from([
            p_uuid("id", &self.id),
            p_uuid("customer_id", &self.customer_id),
            p_str("mandate_ref", &self.mandate_ref),
            p_str("iban", &self.iban),
            p_str("bic", &self.bic),
            p_str("holder_name", &self.holder_name),
            p_str("signed_at", &self.signed_at),
            p_str("status", "active"),
        ]);
        execute_sql(db,
            "INSERT INTO sepa_mandates (id, customer_id, mandate_ref, iban, bic, holder_name, signed_at, status) \
             VALUES (:id, :customer_id, :mandate_ref, :iban, :bic, :holder_name, :signed_at, :status)",
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
    impl ServerCommand for CreateSepaMandate {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            sqlx::query(
                "INSERT INTO sepa_mandates (tenant_id, id, customer_id, mandate_ref, iban, bic, holder_name, signed_at, status) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .bind(DEMO_TENANT_ID)
                .bind(&self.id.0[..])
                .bind(&self.customer_id.0[..])
                .bind(&self.mandate_ref)
                .bind(&self.iban)
                .bind(&self.bic)
                .bind(&self.holder_name)
                .bind(&self.signed_at)
                .bind("active")
                .execute(&mut **tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "INSERT sepa_mandate id={}: {e}",
                    self.id,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
