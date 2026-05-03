use sql_engine::storage::Uuid;
use database::Database;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use rpc_command::rpc_command;
use crate::shared::DEMO_TENANT_ID;

#[rpc_command]
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
    #[ts(type = "string")]
    #[client_default = "nextId()"]
    pub activity_id: Uuid,
    #[client_default = "new Date().toISOString()"]
    pub timestamp: String,
}

fn detail_for(mandate_ref: &str) -> String {
    format!("SEPA-Mandat \"{mandate_ref}\" angelegt")
}

impl Command for CreateSepaMandate {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let detail = detail_for(&self.mandate_ref);
        let mut acc = sql!(
            "INSERT INTO sepa_mandates (id, customer_id, mandate_ref, iban, bic, holder_name, signed_at, status) \
             VALUES ({self.id}, {self.customer_id}, {self.mandate_ref}, {self.iban}, {self.bic}, {self.holder_name}, {self.signed_at}, 'active')"
        )
        .execute(db)?;
        acc.extend(
            sql!(
                "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
                 VALUES ({self.activity_id}, {self.timestamp}, 'sepa', {self.id}, 'create', 'demo', {detail})"
            )
            .execute(db)?,
        );
        Ok(acc)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sea_orm::{DatabaseTransaction, EntityTrait, Set};
    use sync_server_mysql::ServerCommand;

    use crate::activity_log::activity_log_server::insert_activity;
    use crate::sepa_mandates::sepa_mandate_server::entity as sepa_mandate_entity;

    #[async_trait]
    impl ServerCommand for CreateSepaMandate {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let am = sepa_mandate_entity::ActiveModel {
                tenant_id: Set(DEMO_TENANT_ID),
                id: Set(self.id.0.to_vec()),
                customer_id: Set(self.customer_id.0.to_vec()),
                mandate_ref: Set(self.mandate_ref.clone()),
                iban: Set(self.iban.clone()),
                bic: Set(self.bic.clone()),
                holder_name: Set(self.holder_name.clone()),
                signed_at: Set(self.signed_at.clone()),
                status: Set("active".to_string()),
            };
            sepa_mandate_entity::Entity::insert(am)
                .on_conflict_do_nothing()
                .exec_without_returning(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "INSERT sepa_mandate id={}: {e}", self.id,
                )))?;

            let detail = detail_for(&self.mandate_ref);
            insert_activity(
                tx,
                &self.activity_id,
                &self.timestamp,
                "sepa",
                &self.id,
                "create",
                &detail,
            )
            .await?;

            Ok(client_zset.clone())
        }
    }
}
