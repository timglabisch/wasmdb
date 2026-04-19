//! Storage-side infrastructure.
//!
//! `Registry` turns wire-level `(FetcherId, params_bytes)` requests into
//! typed `fetch` calls. Fetchers register a closure via `register::<F>`;
//! no trait impl is needed (sidesteps the orphan rule when client and
//! storage live in different crates).
//!
//! The app-defined `Ext` — typically a struct holding pool handles — is
//! passed straight through. Keeps sqlx (and other backend choices) out
//! of this crate so client-side builds stay WASM-safe.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tables::{FetchRequest, Fetcher};

/// `#[storage]` macro — generates a `pub fn register_{fn}` from a normal
/// `async fn` fetcher. See `tables-macros` for usage.
pub use tables_macros::storage;

#[derive(Debug)]
pub enum StorageError {
    NotRegistered,
    Storage(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::NotRegistered => write!(f, "fetcher not registered"),
            StorageError::Storage(s) => write!(f, "storage: {s}"),
        }
    }
}

impl std::error::Error for StorageError {}

pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Type-erased fetch trampoline. One per registered fetcher. Decodes
/// params from Borsh bytes, calls the typed fetcher, re-encodes rows.
type FetchFn<E> = Box<
    dyn for<'a> Fn(&'a [u8], &'a E) -> BoxFut<'a, Result<Vec<u8>, StorageError>>
        + Send
        + Sync,
>;

/// Storage-side registry. The dispatcher the server uses when a wire
/// request arrives: look up by fetcher id, hand through bytes.
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

    /// Register a fetcher for `F`. The closure receives the typed
    /// params and the app context, returns rows. Borsh framing is
    /// handled by the registry — the closure stays domain-focused.
    pub fn register<F: Fetcher>(
        &mut self,
        fetcher: impl for<'a> Fn(F::Params, &'a E) -> BoxFut<'a, Result<Vec<F::Row>, StorageError>>
            + Send
            + Sync
            + 'static,
    ) {
        let fetcher = Arc::new(fetcher);
        let f: FetchFn<E> = Box::new(move |params_bytes, ctx| {
            let fetcher = fetcher.clone();
            Box::pin(async move {
                let params: F::Params = borsh::from_slice(params_bytes)
                    .map_err(|e| StorageError::Storage(e.to_string()))?;
                let rows = fetcher(params, ctx).await?;
                borsh::to_vec(&rows).map_err(|e| StorageError::Storage(e.to_string()))
            })
        });
        self.fetchers.insert(F::ID.to_string(), f);
    }

    /// Wire-level dispatch. Used by the request handler.
    pub async fn fetch(
        &self,
        fetcher_id: &str,
        params_bytes: &[u8],
        ctx: &E,
    ) -> Result<Vec<u8>, StorageError> {
        let f = self.fetchers.get(fetcher_id).ok_or(StorageError::NotRegistered)?;
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
    registry.fetch(&request.fetcher_id, &request.params, ctx).await
}
