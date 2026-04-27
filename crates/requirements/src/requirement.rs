//! The `DbRequirement` trait — the typed counterpart of [`Requirement`].
//!
//! Renamed from the engine's old `DbCaller`. Emitted by `tables-codegen`
//! per mode (server / client); the trait is mode-agnostic — only the
//! `call` body differs between server and client builds.
//!
//! [`Requirement`]: crate::registry::Requirement

use std::sync::Arc;

use sql_engine::DbTable;
use sql_parser::ast::Value;

use crate::meta::RequirementMeta;
use crate::runtime::RequirementFuture;

/// Query-marker → registrable requirement definition.
///
/// `call` receives owned args (resolved by the caller into concrete
/// `Value`s), converts them into the statically-typed params, runs the
/// underlying work (local `async fn` on the server, HTTP fetch on the
/// client), and returns the rows in `Row::schema()` column order.
pub trait DbRequirement: 'static {
    /// Stable requirement id (wire form `"{schema}::{function}"`),
    /// matches [`Requirement::id`] and the registry key.
    ///
    /// [`Requirement::id`]: crate::registry::Requirement::id
    const ID: &'static str;

    /// App-level context passed to `call`. `()` on the client; typically
    /// a pool/handle wrapper on the server.
    type Ctx: Send + Sync + 'static;

    /// Row type produced by this requirement — determines the `row_table`
    /// into which the runtime upserts results.
    type Row: DbTable;

    /// Static metadata (row_table + positional param shape). Must agree
    /// with the [`RequirementMeta`] stored under the same id in the
    /// [`RequirementRegistry`].
    ///
    /// [`RequirementRegistry`]: crate::registry::RequirementRegistry
    fn meta() -> RequirementMeta;

    /// Execute the requirement. Args are positional, already resolved
    /// to concrete [`Value`]s by the caller.
    fn call(args: Vec<Value>, ctx: Arc<Self::Ctx>) -> RequirementFuture;
}
