use sql_engine::storage::Uuid;
use database::Database;
use sqlbuilder::sql;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use rpc_command::rpc_command;
use crate::command_helpers::SqlStmtExt;
use crate::shared::DEMO_TENANT_ID;

#[rpc_command]
pub struct CreateRecurring {
    #[ts(type = "string")]
    pub id: Uuid,
    #[ts(type = "string")]
    pub customer_id: Uuid,
    pub template_name: String,
    pub interval_unit: String,
    #[ts(type = "number")]
    pub interval_value: i64,
    pub next_run: String,
    pub status_template: String,
    pub notes_template: String,
    #[ts(type = "string")]
    #[client_default = "nextId()"]
    pub activity_id: Uuid,
    #[client_default = "new Date().toISOString()"]
    pub timestamp: String,
}

fn detail_for(template_name: &str) -> String {
    format!("Serie \"{template_name}\" angelegt")
}

impl Command for CreateRecurring {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let detail = detail_for(&self.template_name);
        let mut acc = sql!(
            "INSERT INTO recurring_invoices (id, customer_id, template_name, interval_unit, interval_value, next_run, last_run, enabled, status_template, notes_template) \
             VALUES ({self.id}, {self.customer_id}, {self.template_name}, {self.interval_unit}, {self.interval_value}, {self.next_run}, '', 1, {self.status_template}, {self.notes_template})"
        )
        .execute(db)?;
        acc.extend(
            sql!(
                "INSERT INTO activity_log (id, timestamp, entity_type, entity_id, action, actor, detail) \
                 VALUES ({self.activity_id}, {self.timestamp}, 'recurring', {self.id}, 'create', 'demo', {detail})"
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
    use crate::recurring::recurring_invoice_server::entity as recurring_invoice_entity;

    #[async_trait]
    impl ServerCommand for CreateRecurring {
        async fn execute_server(
            &self,
            tx: &DatabaseTransaction,
            client_zset: &ZSet,
        ) -> Result<ZSet, CommandError> {
            let am = recurring_invoice_entity::ActiveModel {
                tenant_id: Set(DEMO_TENANT_ID),
                id: Set(self.id.0.to_vec()),
                customer_id: Set(self.customer_id.0.to_vec()),
                template_name: Set(self.template_name.clone()),
                interval_unit: Set(self.interval_unit.clone()),
                interval_value: Set(self.interval_value),
                next_run: Set(self.next_run.clone()),
                last_run: Set(String::new()),
                enabled: Set(1),
                status_template: Set(self.status_template.clone()),
                notes_template: Set(self.notes_template.clone()),
            };
            recurring_invoice_entity::Entity::insert(am)
                .on_conflict_do_nothing()
                .exec_without_returning(tx)
                .await
                .map_err(|e| CommandError::ExecutionFailed(format!(
                    "INSERT recurring_invoice {}: {e}",
                    self.id,
                )))?;

            let detail = detail_for(&self.template_name);
            insert_activity(
                tx,
                &self.activity_id,
                &self.timestamp,
                "recurring",
                &self.id,
                "create",
                &detail,
            )
            .await?;

            Ok(client_zset.clone())
        }
    }
}
