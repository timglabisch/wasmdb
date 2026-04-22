use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct UpdateSepaMandate {
    pub id: i64,
    pub mandate_ref: String,
    pub iban: String,
    pub bic: String,
    pub holder_name: String,
    pub signed_at: String,
    pub status: String,
}

impl Command for UpdateSepaMandate {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let params = Params::from([
            p_int("id", self.id),
            p_str("mandate_ref", &self.mandate_ref),
            p_str("iban", &self.iban),
            p_str("bic", &self.bic),
            p_str("holder_name", &self.holder_name),
            p_str("signed_at", &self.signed_at),
            p_str("status", &self.status),
        ]);
        execute_sql(db,
            "UPDATE sepa_mandates SET mandate_ref = :mandate_ref, iban = :iban, bic = :bic, holder_name = :holder_name, signed_at = :signed_at, status = :status WHERE sepa_mandates.id = :id",
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
    impl ServerCommand for UpdateSepaMandate {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            sqlx::query(
                "UPDATE sepa_mandates SET mandate_ref = ?, iban = ?, bic = ?, holder_name = ?, signed_at = ?, status = ? WHERE id = ?")
                .bind(&self.mandate_ref)
                .bind(&self.iban)
                .bind(&self.bic)
                .bind(&self.holder_name)
                .bind(&self.signed_at)
                .bind(&self.status)
                .bind(self.id)
                .execute(&mut **tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "UPDATE sepa_mandate id={}: {e}",
                    self.id,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
