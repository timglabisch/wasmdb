use sql_engine::storage::Uuid;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use rpc_command::rpc_command;
use crate::command_helpers::{execute_sql, p_str, p_uuid};

#[rpc_command]
pub struct UpdateSepaMandate {
    #[ts(type = "string")]
    pub id: Uuid,
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
            p_uuid("id", &self.id),
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
    use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter, Set};
    use sync_server_mysql::ServerCommand;

    use crate::sepa_mandates::sepa_mandate_server::entity as sepa_mandate_entity;
    use crate::shared::DEMO_TENANT_ID;

    #[async_trait]
    impl ServerCommand for UpdateSepaMandate {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let model = sepa_mandate_entity::Entity::find()
                .filter(sepa_mandate_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(sepa_mandate_entity::Column::Id.eq(self.id.0.to_vec()))
                .one(tx).await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "load sepa_mandate {}: {e}", self.id,
                )))?
                .ok_or_else(|| CommandError::ExecutionFailed(format!(
                    "sepa_mandate {} not found", self.id,
                )))?;

            let mut am: sepa_mandate_entity::ActiveModel = model.into();
            am.mandate_ref = Set(self.mandate_ref.clone());
            am.iban = Set(self.iban.clone());
            am.bic = Set(self.bic.clone());
            am.holder_name = Set(self.holder_name.clone());
            am.signed_at = Set(self.signed_at.clone());
            am.status = Set(self.status.clone());
            am.update(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                "UPDATE sepa_mandate {}: {e}", self.id,
            )))?;
            Ok(client_zset.clone())
        }
    }
}
