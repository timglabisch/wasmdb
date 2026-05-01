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
    use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter};
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
            sepa_mandate_entity::Entity::update_many()
                .col_expr(sepa_mandate_entity::Column::MandateRef, self.mandate_ref.clone().into())
                .col_expr(sepa_mandate_entity::Column::Iban, self.iban.clone().into())
                .col_expr(sepa_mandate_entity::Column::Bic, self.bic.clone().into())
                .col_expr(sepa_mandate_entity::Column::HolderName, self.holder_name.clone().into())
                .col_expr(sepa_mandate_entity::Column::SignedAt, self.signed_at.clone().into())
                .col_expr(sepa_mandate_entity::Column::Status, self.status.clone().into())
                .filter(sepa_mandate_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(sepa_mandate_entity::Column::Id.eq(self.id.0.to_vec()))
                .exec(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "UPDATE sepa_mandate id={}: {e}", self.id,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
