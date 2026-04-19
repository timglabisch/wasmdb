//! Storage-side infrastructure.
//!
//! `StorageTable` is the typed contract each table implements. `Registry`
//! is the runtime that turns wire-level `(TableId, params_bytes)`
//! requests into typed `fetch` calls via registered trampolines.
//!
//! The app-defined `Ext` — typically a struct holding pool handles — is
//! passed straight through. Keeps sqlx (and other backend choices) out
//! of this crate so client-side builds stay WASM-safe.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use tables::{FetchRequest, Table};

#[derive(Debug)]
pub enum StorageError {
    NotRegistered,
    Storage(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::NotRegistered => write!(f, "table not registered"),
            StorageError::Storage(s) => write!(f, "storage: {s}"),
        }
    }
}

impl std::error::Error for StorageError {}

pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Storage-side counterpart to `tables::Table`.
pub trait StorageTable: Table {
    type Ext: Send + Sync + 'static;
    fn fetch(
        params: Self::Params,
        ctx: &Self::Ext,
    ) -> BoxFut<'_, Result<Vec<Self::Row>, StorageError>>;
}

/// Type-erased fetch trampoline. One per registered table. Decodes
/// params from Borsh bytes, calls the typed `fetch`, re-encodes rows.
type FetchFn<E> = Box<
    dyn for<'a> Fn(&'a [u8], &'a E) -> BoxFut<'a, Result<Vec<u8>, StorageError>>
        + Send
        + Sync,
>;

/// Storage-side registry. The dispatcher the server uses when a wire
/// request arrives: look up by table id, hand through bytes.
pub struct Registry<E> {
    fetchers: HashMap<String, FetchFn<E>>,
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
        self.fetchers.insert(T::ID.to_string(), f);
    }

    /// Wire-level dispatch. Used by the request handler.
    pub async fn fetch(
        &self,
        table_id: &str,
        params_bytes: &[u8],
        ctx: &E,
    ) -> Result<Vec<u8>, StorageError> {
        let f = self.fetchers.get(table_id).ok_or(StorageError::NotRegistered)?;
        f(params_bytes, ctx).await
    }
}

/// Wire-level entry point: take a Borsh-encoded `FetchRequest` body,
/// dispatch via the registry, return Borsh-encoded rows. The HTTP layer
/// (axum or otherwise) just pipes bytes in and bytes out.
pub async fn handle_fetch_bytes<E: Send + Sync + 'static>(
    registry: &Registry<E>,
    body_bytes: &[u8],
    ctx: &E,
) -> Result<Vec<u8>, StorageError> {
    let request: FetchRequest = borsh::from_slice(body_bytes)
        .map_err(|e| StorageError::Storage(e.to_string()))?;
    registry.fetch(&request.table_id, &request.params, ctx).await
}
