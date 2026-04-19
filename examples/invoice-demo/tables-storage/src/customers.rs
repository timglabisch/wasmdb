//! Storage-side `Customers`. Has its own marker struct (orphan rule)
//! with the same `TableId` as the client-side marker, so Registry
//! lookups match. Params/Row types are reused from the client crate.

use invoice_demo_tables_client::customers::{CustomersParams, CustomersRow};
use tables::{Table, TableId};
use tables_storage::{BoxFut, StorageError, StorageTable};

use crate::AppCtx;

pub struct Customers;

impl Table for Customers {
    const ID: TableId = "invoice_demo::Customers";
    type Params = CustomersParams;
    type Row = CustomersRow;
}

impl StorageTable for Customers {
    type Ext = AppCtx;
    fn fetch(
        params: CustomersParams,
        ctx: &AppCtx,
    ) -> BoxFut<'_, Result<Vec<CustomersRow>, StorageError>> {
        Box::pin(async move {
            let rows: Vec<(i64, String)> = sqlx::query_as(
                "SELECT id, name FROM invoice_demo.customers WHERE owner_id = ?",
            )
            .bind(params.owner_id)
            .fetch_all(&ctx.pool)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

            Ok(rows
                .into_iter()
                .map(|(id, name)| CustomersRow { id, name })
                .collect())
        })
    }
}
