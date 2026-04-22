use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int, p_str};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct CreateRecurring {
    pub id: i64,
    pub customer_id: i64,
    pub template_name: String,
    pub interval_unit: String,
    pub interval_value: i64,
    pub next_run: String,
    pub status_template: String,
    pub notes_template: String,
}

impl Command for CreateRecurring {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let params = Params::from([
            p_int("id", self.id),
            p_int("customer_id", self.customer_id),
            p_str("template_name", &self.template_name),
            p_str("interval_unit", &self.interval_unit),
            p_int("interval_value", self.interval_value),
            p_str("next_run", &self.next_run),
            p_str("last_run", ""),
            p_int("enabled", 1),
            p_str("status_template", &self.status_template),
            p_str("notes_template", &self.notes_template),
        ]);
        execute_sql(db,
            "INSERT INTO recurring_invoices (id, customer_id, template_name, interval_unit, interval_value, next_run, last_run, enabled, status_template, notes_template) \
             VALUES (:id, :customer_id, :template_name, :interval_unit, :interval_value, :next_run, :last_run, :enabled, :status_template, :notes_template)",
            params)
    }
}

#[cfg(feature = "server")]
mod server_impl {
    use super::*;
    use std::collections::HashMap;
    use async_trait::async_trait;
    use sql_engine::schema::TableSchema;
    use sqlx::{MySql, Transaction};
    use sync_server_mysql::{apply_zset, ServerCommand};

    #[async_trait]
    impl ServerCommand for CreateRecurring {
        async fn execute_server(
            &self,
            tx: &mut Transaction<'static, MySql>,
            client_zset: &ZSet,
            schemas: &HashMap<String, TableSchema>,
        ) -> Result<ZSet, CommandError> {
            apply_zset(tx, client_zset, schemas).await?;
            Ok(client_zset.clone())
        }
    }
}
