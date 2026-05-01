use sql_engine::storage::Uuid;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use rpc_command::rpc_command;
use crate::command_helpers::{execute_sql, p_int, p_str, p_uuid};
use crate::shared::DEMO_TENANT_ID;

#[rpc_command]
pub struct UpdateRecurring {
    #[ts(type = "string")]
    pub id: Uuid,
    pub template_name: String,
    pub interval_unit: String,
    #[ts(type = "number")]
    pub interval_value: i64,
    pub next_run: String,
    #[ts(type = "number")]
    pub enabled: i64,
    pub status_template: String,
    pub notes_template: String,
}

impl Command for UpdateRecurring {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let params = Params::from([
            p_uuid("id", &self.id),
            p_str("template_name", &self.template_name),
            p_str("interval_unit", &self.interval_unit),
            p_int("interval_value", self.interval_value),
            p_str("next_run", &self.next_run),
            p_int("enabled", self.enabled),
            p_str("status_template", &self.status_template),
            p_str("notes_template", &self.notes_template),
        ]);
        execute_sql(db,
            "UPDATE recurring_invoices SET template_name = :template_name, interval_unit = :interval_unit, interval_value = :interval_value, next_run = :next_run, enabled = :enabled, status_template = :status_template, notes_template = :notes_template WHERE recurring_invoices.id = :id",
            params)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use async_trait::async_trait;
    use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter, Set};
    use sync_server_mysql::ServerCommand;

    use crate::recurring::recurring_invoice_server::entity as recurring_invoice_entity;

    #[async_trait]
    impl ServerCommand for UpdateRecurring {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let model = recurring_invoice_entity::Entity::find()
                .filter(recurring_invoice_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(recurring_invoice_entity::Column::Id.eq(self.id.0.to_vec()))
                .one(tx).await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "load recurring {}: {e}", self.id,
                )))?
                .ok_or_else(|| CommandError::ExecutionFailed(format!(
                    "recurring {} not found", self.id,
                )))?;

            let mut am: recurring_invoice_entity::ActiveModel = model.into();
            am.template_name = Set(self.template_name.clone());
            am.interval_unit = Set(self.interval_unit.clone());
            am.interval_value = Set(self.interval_value);
            am.next_run = Set(self.next_run.clone());
            am.enabled = Set(self.enabled);
            am.status_template = Set(self.status_template.clone());
            am.notes_template = Set(self.notes_template.clone());
            am.update(tx).await.map_err(|e| CommandError::ExecutionFailed(format!(
                "UPDATE recurring {}: {e}", self.id,
            )))?;
            Ok(client_zset.clone())
        }
    }
}
