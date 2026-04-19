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
use tables_storage::{StorageTable, StorageCtx, StorageError};

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

impl StorageTable for Customers {
    fn fetch(params: &CustomersParams, ctx: &StorageCtx) -> Result<Vec<CustomersRow>, StorageError> {
        if params.owner_id != ctx.session_owner_id {
            return Err(StorageError::Unauthorized);
        }
        // real impl: sqlx against TiDB with WHERE owner_id = ?
        Ok(Vec::new())
    }
}
