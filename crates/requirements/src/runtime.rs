//! Runtime types for invoking a requirement: the future shape and the
//! type-erased boxed closure consumed by the registry.
//!
//! Closures are self-contained — they capture their dependencies (db
//! handle, http client, etc.) at registration time and write fetched
//! rows directly. The future yields `()` on success; the dispatcher only
//! needs to know whether the fetch finished or failed.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use sql_parser::ast::Value;

/// Future returned by an async requirement fetcher. Receives owned args
/// (so the future can outlive the call site) and yields unit on success
/// — rows are written directly to the local store by the closure.
///
/// Native builds carry `+ Send` so a runtime holding requirements stays
/// `Send`. The wasm client runs single-threaded and HTTP fetchers use
/// `JsFuture` — `Rc<RefCell<_>>` is `!Send` — so the bound is dropped
/// there.
#[cfg(not(target_arch = "wasm32"))]
pub type RequirementFuture =
    Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;

#[cfg(target_arch = "wasm32")]
pub type RequirementFuture =
    Pin<Box<dyn Future<Output = Result<(), String>>>>;

/// A registered requirement fetcher. `Arc` so cloning shares closure
/// identity rather than forcing every closure to be `Clone`.
#[cfg(not(target_arch = "wasm32"))]
pub type RequirementFn = Arc<dyn Fn(Vec<Value>) -> RequirementFuture + Send + Sync>;

#[cfg(target_arch = "wasm32")]
pub type RequirementFn = Arc<dyn Fn(Vec<Value>) -> RequirementFuture>;
