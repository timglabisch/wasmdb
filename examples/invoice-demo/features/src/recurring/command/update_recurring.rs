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
    use sea_orm::{ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter};
    use sync_server_mysql::ServerCommand;

    use crate::recurring::recurring_invoice_server::entity as recurring_invoice_entity;

    #[async_trait]
    impl ServerCommand for UpdateRecurring {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            recurring_invoice_entity::Entity::update_many()
                .col_expr(recurring_invoice_entity::Column::TemplateName, self.template_name.clone().into())
                .col_expr(recurring_invoice_entity::Column::IntervalUnit, self.interval_unit.clone().into())
                .col_expr(recurring_invoice_entity::Column::IntervalValue, self.interval_value.into())
                .col_expr(recurring_invoice_entity::Column::NextRun, self.next_run.clone().into())
                .col_expr(recurring_invoice_entity::Column::Enabled, self.enabled.into())
                .col_expr(recurring_invoice_entity::Column::StatusTemplate, self.status_template.clone().into())
                .col_expr(recurring_invoice_entity::Column::NotesTemplate, self.notes_template.clone().into())
                .filter(recurring_invoice_entity::Column::TenantId.eq(DEMO_TENANT_ID))
                .filter(recurring_invoice_entity::Column::Id.eq(self.id.0.to_vec()))
                .exec(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "UPDATE recurring_invoice {}: {e}",
                    self.id,
                )))?;
            Ok(client_zset.clone())
        }
    }
}
