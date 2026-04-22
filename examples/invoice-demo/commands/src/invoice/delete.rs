use borsh::{BorshSerialize, BorshDeserialize};
use serde::{Serialize, Deserialize};
use ts_rs::TS;
use database::Database;
use sql_engine::execute::Params;
use sync::command::{Command, CommandError};
use sync::zset::ZSet;
use crate::helpers::{execute_sql, p_int};

/// Cascades positions + payments + invoice — all in one atomic ZSet.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, Serialize, Deserialize, TS)]
pub struct DeleteInvoice {
    pub id: i64,
}

impl Command for DeleteInvoice {
    fn execute_optimistic(
        &self,
        db: &mut Database,
    ) -> Result<ZSet, CommandError> {
        let id = self.id;
        let mut acc = ZSet::new();
        let p = Params::from([p_int("iid", id)]);
        acc.extend(execute_sql(db,
            "DELETE FROM payments WHERE invoice_id = :iid", p)?);
        let p = Params::from([p_int("iid", id)]);
        acc.extend(execute_sql(db,
            "DELETE FROM positions WHERE invoice_id = :iid", p)?);
        let p = Params::from([p_int("id", id)]);
        acc.extend(execute_sql(db,
            "DELETE FROM invoices WHERE id = :id", p)?);
        Ok(acc)
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
    impl ServerCommand for DeleteInvoice {
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
