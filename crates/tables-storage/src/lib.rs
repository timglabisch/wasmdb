//! Storage-side infrastructure.
//!
//! `StorageTable` is the typed contract each table implements. `Registry`
//! is the runtime that turns wire-level `(TableId, params_bytes)`
//! requests into typed `fetch` calls via registered trampolines.
//!
//! `StorageCtx<E>` carries identity (session_owner_id) plus an
//! app-defined `ext` — typically a struct holding pool handles. This
//! keeps sqlx (and other backend choices) out of the infra crate so
//! client-side builds stay WASM-safe.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use tables::{Table, TableId};

pub struct StorageCtx<E> {
    pub session_owner_id: i64,
    pub ext: E,
}

#[derive(Debug)]
pub enum StorageError {
    Unauthorized,
    NotRegistered,
    Storage(String),
}

pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Storage-side counterpart to `tables::Table`. `fetch` authorizes and
/// produces rows in one step — the typical auth check falls out of the
/// WHERE clause anyway.
pub trait StorageTable: Table {
    type Ext: Send + Sync + 'static;
    fn fetch(
        params: Self::Params,
        ctx: &StorageCtx<Self::Ext>,
    ) -> BoxFut<'_, Result<Vec<Self::Row>, StorageError>>;
}

/// Type-erased fetch trampoline. One per registered table. Decodes
/// params from Borsh bytes, calls the typed `fetch`, re-encodes rows.
type FetchFn<E> = Box<
    dyn for<'a> Fn(&'a [u8], &'a StorageCtx<E>) -> BoxFut<'a, Result<Vec<u8>, StorageError>>
        + Send
        + Sync,
>;

/// Storage-side registry. The dispatcher the server uses when a wire
/// request arrives: look up by `TableId`, hand through bytes.
pub struct Registry<E> {
    fetchers: HashMap<TableId, FetchFn<E>>,
}

impl<E> Default for Registry<E> {
    fn default() -> Self {
        Self { fetchers: HashMap::new() }
    }
}

impl<E: Send + Sync + 'static> Registry<E> {
    pub fn new() -> Self { Self::default() }

    /// Register a table. Called once per table at server startup.
    pub fn register<T: StorageTable<Ext = E>>(&mut self) {
        let f: FetchFn<E> = Box::new(|params_bytes, ctx| {
            Box::pin(async move {
                let params: T::Params = borsh::from_slice(params_bytes)
                    .map_err(|e| StorageError::Storage(e.to_string()))?;
                let rows = T::fetch(params, ctx).await?;
                borsh::to_vec(&rows).map_err(|e| StorageError::Storage(e.to_string()))
            })
        });
        self.fetchers.insert(T::ID, f);
    }

    /// Wire-level dispatch. Used by the request handler.
    pub async fn fetch(
        &self,
        table_id: TableId,
        params_bytes: &[u8],
        ctx: &StorageCtx<E>,
    ) -> Result<Vec<u8>, StorageError> {
        let f = self.fetchers.get(table_id).ok_or(StorageError::NotRegistered)?;
        f(params_bytes, ctx).await
    }
}
