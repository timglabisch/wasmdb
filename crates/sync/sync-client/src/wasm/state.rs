//! Singleton CLIENT for the wasm app, type-erased so non-generic
//! `#[wasm_bindgen]` exports in this crate can reach the parts of
//! `SyncClient` whose return types do not mention `C` (the database,
//! stream IDs, debug snapshots). Generic-over-`C` callers downcast
//! through `as_any_mut()`.

use std::any::Any;
use std::cell::RefCell;

use database_reactive::ReactiveDatabase;
use sync::command::Command;
use sync::protocol::StreamId;

use crate::client::SyncClient;

/// Object-safe surface that the wasm exports actually need. Anything
/// touching `CommandRequest<C>` stays out of this trait — those callers
/// downcast to `SyncClient<C>` via `as_any_mut`.
pub trait DynClient {
    fn db(&self) -> &ReactiveDatabase;
    fn db_mut(&mut self) -> &mut ReactiveDatabase;
    fn create_stream(&mut self) -> StreamId;
    fn stream_count(&self) -> usize;
    fn total_pending(&self) -> usize;
    fn stream_pending_detail(&self) -> Vec<(StreamId, Vec<(u64, usize)>)>;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<C: Command + 'static> DynClient for SyncClient<C> {
    fn db(&self) -> &ReactiveDatabase {
        SyncClient::db(self)
    }
    fn db_mut(&mut self) -> &mut ReactiveDatabase {
        SyncClient::db_mut(self)
    }
    fn create_stream(&mut self) -> StreamId {
        SyncClient::create_stream(self)
    }
    fn stream_count(&self) -> usize {
        SyncClient::stream_count(self)
    }
    fn total_pending(&self) -> usize {
        SyncClient::total_pending(self)
    }
    fn stream_pending_detail(&self) -> Vec<(StreamId, Vec<(u64, usize)>)> {
        SyncClient::stream_pending_detail(self)
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

thread_local! {
    static CLIENT: RefCell<Option<Box<dyn DynClient>>> = const { RefCell::new(None) };
    pub(crate) static DEFAULT_STREAM_ID: RefCell<Option<u64>> = const { RefCell::new(None) };
}

pub fn install_client<C: Command + 'static>(client: SyncClient<C>) {
    CLIENT.with(|c| *c.borrow_mut() = Some(Box::new(client)));
}

/// Non-generic accessor — use this from `#[wasm_bindgen]` exports that
/// only need the trait surface (db, streams, debug snapshots).
pub fn with_client_dyn<R>(f: impl FnOnce(&mut dyn DynClient) -> R) -> R {
    CLIENT.with(|c| {
        let mut borrow = c.borrow_mut();
        let client = borrow
            .as_mut()
            .expect("sync-client wasm: client not installed — call init() first");
        f(client.as_mut())
    })
}

/// Typed accessor — required when touching `CommandRequest<C>` (the
/// execute paths and the stream queue). The macro-emitted entry points
/// supply `C` here.
pub fn with_client<C: Command + 'static, R>(
    f: impl FnOnce(&mut SyncClient<C>) -> R,
) -> R {
    CLIENT.with(|c| {
        let mut borrow = c.borrow_mut();
        let client = borrow
            .as_mut()
            .expect("sync-client wasm: client not installed — call init() first");
        let any = client.as_any_mut();
        let typed: &mut SyncClient<C> = any
            .downcast_mut()
            .expect("sync-client wasm: command-type mismatch on with_client");
        f(typed)
    })
}

pub fn set_default_stream_id(id: u64) {
    DEFAULT_STREAM_ID.with(|d| *d.borrow_mut() = Some(id));
}

pub fn default_stream_id() -> u64 {
    DEFAULT_STREAM_ID
        .with(|d| *d.borrow())
        .expect("sync-client wasm: default stream not set — call init() first")
}
