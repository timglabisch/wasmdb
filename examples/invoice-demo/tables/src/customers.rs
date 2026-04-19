//! `Customers` — one instance per `owner_id`.
//!
//! Target shape with future macro:
//! ```ignore
//! #[table]
//! pub struct Customers {
//!     #[param] pub owner_id: i64,
//!     #[pk]    pub id: i64,
//!     pub name: String,
//! }
//! ```

use borsh::{BorshSerialize, BorshDeserialize};
use tables::{Params, Row, Table, TableId};

pub struct Customers;

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct CustomersParams {
    pub owner_id: i64,
}
impl Params for CustomersParams {}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct CustomersRow {
    pub id: i64,
    pub name: String,
}
impl Row for CustomersRow {
    type Pk = i64;
    fn pk(&self) -> i64 { self.id }
}

impl Table for Customers {
    const ID: TableId = "invoice_demo::Customers";
    type Params = CustomersParams;
    type Row = CustomersRow;
}

#[cfg(feature = "storage")]
mod storage_impl {
    use super::*;
    use tables_storage::{BoxFut, StorageCtx, StorageError, StorageTable};
    use crate::AppCtx;

    impl StorageTable for Customers {
        type Ext = AppCtx;
        fn fetch(
            params: CustomersParams,
            ctx: &StorageCtx<AppCtx>,
        ) -> BoxFut<'_, Result<Vec<CustomersRow>, StorageError>> {
            Box::pin(async move {
                if params.owner_id != ctx.session_owner_id {
                    return Err(StorageError::Unauthorized);
                }
                let rows: Vec<(i64, String)> = sqlx::query_as(
                    "SELECT id, name FROM invoice_demo.customers WHERE owner_id = ?",
                )
                .bind(params.owner_id)
                .fetch_all(&ctx.ext.pool)
                .await
                .map_err(|e| StorageError::Storage(e.to_string()))?;

                Ok(rows
                    .into_iter()
                    .map(|(id, name)| CustomersRow { id, name })
                    .collect())
            })
        }
    }
}
