//! Runtime types for invoking a requirement: the future shape and the
//! type-erased boxed closure consumed by the registry.
//!
//! Renamed from the engine's old `AsyncFetcherFn` / `FetcherFuture`. The
//! shape — owned-args in, full rows out — is unchanged. Runtime store
//! mechanics (state machine, refcount, GC) build on top.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use sql_engine::storage::CellValue;
use sql_parser::ast::Value;

/// Future returned by an async requirement fetcher. Receives owned args
/// (so the future can outlive the call site) and yields full rows — cells
/// in the `row_table`'s column order.
///
/// Native builds carry `+ Send` so a runtime holding requirements stays
/// `Send`. The wasm client runs single-threaded and HTTP fetchers use
/// `JsFuture` — `Rc<RefCell<_>>` is `!Send` — so the bound is dropped
/// there.
#[cfg(not(target_arch = "wasm32"))]
pub type RequirementFuture =
    Pin<Box<dyn Future<Output = Result<Vec<Vec<CellValue>>, String>> + Send>>;

#[cfg(target_arch = "wasm32")]
pub type RequirementFuture =
    Pin<Box<dyn Future<Output = Result<Vec<Vec<CellValue>>, String>>>>;

/// A registered requirement fetcher. `Arc` so cloning shares closure
/// identity rather than forcing every closure to be `Clone`.
#[cfg(not(target_arch = "wasm32"))]
pub type RequirementFn = Arc<dyn Fn(Vec<Value>) -> RequirementFuture + Send + Sync>;

#[cfg(target_arch = "wasm32")]
pub type RequirementFn = Arc<dyn Fn(Vec<Value>) -> RequirementFuture>;
