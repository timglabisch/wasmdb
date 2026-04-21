//! Registered callers — the planner+executor bundle for `schema.fn(args)`
//! FROM-sources.
//!
//! A caller has two halves that always come in as one value:
//! - planner-side `RequirementMeta` (row_table + param shape, consulted
//!   at plan time) — lives in [`crate::planner::requirement`].
//! - executor-side `AsyncFetcherFn` (the closure Phase 0 awaits) —
//!   lives in [`crate::execute::requirement`].
//!
//! The two halves must never drift apart — the planner only accepts a
//! call whose meta is registered, and Phase 0 only invokes fetchers whose
//! meta the planner already recognized. Bundling them into [`Caller`]
//! makes half-registration impossible at the API boundary.
//!
//! [`CallerRegistry`] still stores them in two parallel maps — not one
//! `HashMap<id, (meta, fn)>` — because the planner wants `&RequirementRegistry`
//! and the executor wants `&FetcherRuntime` as separate borrows, with zero
//! projection step.

use std::sync::Arc;

use sql_parser::ast::Value;

use crate::execute::{AsyncFetcherFn, FetcherFuture, FetcherRuntime};
use crate::planner::requirement::{RequirementMeta, RequirementRegistry};
use crate::schema::TableSchema;
use crate::storage::CellValue;

/// A single registered caller: planner meta + async fetcher, keyed by
/// `id` (wire-form `"{schema}::{function}"`).
pub struct Caller {
    pub id: String,
    pub meta: RequirementMeta,
    pub fetcher: AsyncFetcherFn,
}

impl Caller {
    pub fn new(
        id: impl Into<String>,
        meta: RequirementMeta,
        fetcher: AsyncFetcherFn,
    ) -> Self {
        Self { id: id.into(), meta, fetcher }
    }
}

/// Registry of callers. Stores planner view and executor view as parallel
/// `pub` maps so consumers can take disjoint field borrows (`&requirements`
/// + `&fetchers`, or `&mut some_other_field` + `&fetchers`) without a helper.
/// `Clone` shares closure identity via `Arc` inside [`AsyncFetcherFn`].
#[derive(Clone, Default)]
pub struct CallerRegistry {
    pub requirements: RequirementRegistry,
    pub fetchers: FetcherRuntime,
}

impl CallerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, caller: Caller) {
        let Caller { id, meta, fetcher } = caller;
        self.requirements.insert(id.clone(), meta);
        self.fetchers.insert(id, fetcher);
    }
}

/// Rust-Type → Row in a Database table. Provides the `TableSchema` used
/// at `register_table` time plus a cell projection (`into_cells`) in
/// the schema's column order, so typed rows can be fed into the
/// engine's `Vec<CellValue>` storage without hand-writing glue.
///
/// Emitted by the `#[row]` proc-macro on the original struct and by
/// `tables-codegen` on the client-side duplicate. Both produce an impl
/// with identical semantics — server and client share the row shape.
pub trait DbTable: Sized {
    /// Stable table name used as both `TableSchema::name` and the
    /// lookup key in `Database::tables`. Convention: `snake_case` of
    /// the Rust struct name.
    const TABLE: &'static str;

    /// Full schema for this row type. Callers register it via
    /// `Database::register_table::<Self>()`.
    fn schema() -> TableSchema;

    /// Project one instance into a cell row in `schema().columns`
    /// order. Hook for typed insert paths; Phase 0 still writes
    /// `Vec<CellValue>` directly from fetcher output, so this is
    /// unused today but keeps the surface symmetric.
    fn into_cells(self) -> Vec<CellValue>;
}

/// Query-marker → registrable caller definition. `call` receives the
/// `Vec<Value>` args Phase 0 resolved, converts them into the statically-
/// typed params, runs the underlying work (local `async fn` on the
/// server, HTTP fetch on the client), and returns the rows as
/// `Vec<Vec<CellValue>>` in `Row::schema()` column order.
///
/// Emitted by `tables-codegen` per mode — the trait is mode-agnostic,
/// only the `call` body differs between server and client builds.
pub trait DbCaller: 'static {
    /// Stable caller id (wire form `"{schema}::{function}"`), matches
    /// `Caller::id` and `RequirementMeta`-based planner resolution.
    const ID: &'static str;

    /// App-level context passed to `call`. `()` on the client;
    /// typically a pool/handle wrapper on the server.
    type Ctx: Send + Sync + 'static;

    /// Row type produced by this caller — determines the row_table
    /// into which Phase 0 upserts results.
    type Row: DbTable;

    /// Planner metadata (row_table + positional param shape). Must
    /// agree with the `RequirementMeta` stored in the planner's
    /// `RequirementRegistry` at plan time.
    fn meta() -> RequirementMeta;

    /// Execute the caller. Args are positional, already resolved by
    /// Phase 0 from `bound_values` + external params.
    fn call(args: Vec<Value>, ctx: Arc<Self::Ctx>) -> FetcherFuture;
}
