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

use crate::execute::{AsyncFetcherFn, FetcherRuntime};
use crate::planner::requirement::{RequirementMeta, RequirementRegistry};

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
