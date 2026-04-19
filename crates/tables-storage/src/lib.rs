//! Storage-side infrastructure.
//!
//! `StorageTable` is the typed contract each table implements. `Registry`
//! is the runtime that turns wire-level `(TableId, params_bytes)`
//! requests into typed `fetch` calls via registered trampolines.

use std::collections::HashMap;
use tables::{Table, TableId};

pub struct StorageCtx {
    pub session_owner_id: i64,
}

#[derive(Debug)]
pub enum StorageError {
    Unauthorized,
    NotRegistered,
    Storage(String),
}

/// Storage-side counterpart to `tables::Table`. `fetch` authorizes and
/// produces rows in one step — the typical auth check falls out of the
/// WHERE clause anyway.
pub trait StorageTable: Table {
    fn fetch(params: &Self::Params, ctx: &StorageCtx) -> Result<Vec<Self::Row>, StorageError>;
}

/// Type-erased fetch trampoline. One per registered table. Decodes
/// params from Borsh bytes, calls the typed `fetch`, re-encodes rows.
type FetchFn = Box<dyn Fn(&[u8], &StorageCtx) -> Result<Vec<u8>, StorageError> + Send + Sync>;

/// Storage-side registry. The dispatcher the server uses when a wire
/// request arrives: look up by `TableId`, hand through bytes.
#[derive(Default)]
pub struct Registry {
    fetchers: HashMap<TableId, FetchFn>,
}

impl Registry {
    pub fn new() -> Self { Self::default() }

    /// Register a table. Called once per table at server startup.
    pub fn register<T: StorageTable>(&mut self) {
        let f: FetchFn = Box::new(|params_bytes, ctx| {
            let params: T::Params = borsh::from_slice(params_bytes)
                .map_err(|e| StorageError::Storage(e.to_string()))?;
            let rows = T::fetch(&params, ctx)?;
            borsh::to_vec(&rows).map_err(|e| StorageError::Storage(e.to_string()))
        });
        self.fetchers.insert(T::ID, f);
    }

    /// Wire-level dispatch. Used by the request handler.
    pub fn fetch(&self, table_id: TableId, params_bytes: &[u8], ctx: &StorageCtx) -> Result<Vec<u8>, StorageError> {
        self.fetchers.get(table_id)
            .ok_or(StorageError::NotRegistered)?
            (params_bytes, ctx)
    }
}
