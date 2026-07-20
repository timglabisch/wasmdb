//! Materialized views as Rust functions — the projection layer between
//! `database` and `database-reactive`.
//!
//! A projection is a PURE function from source-table rows to derived-table
//! rows, split by a partition column. The engine keeps derived tables in
//! sync with their sources by recomputing affected partitions whenever a
//! batch of changes touches a source table, diffing the new render against
//! the last one (`last_render`), and applying only the delta. See
//! `docs/wasmdb-projections-design.md` for the full design.
//!
//! Contracts the engine relies on (and enforces where it can):
//!
//! - **Purity**: `project` must be deterministic and side-effect free; its
//!   returned rows are its only effect channel.
//! - **State-based invariant**: derived tables ≡ `project(current source
//!   rows)` — regardless of HOW the sources changed (append, update,
//!   delete, compaction).
//! - **Ownership**: an output table is written exclusively by the engine on
//!   behalf of exactly one projection. External writes are rejected via
//!   [`ProjectionEngine::guard_external`].
//! - **Bookkeeping = `last_render`, no delta journal**: the sum of all
//!   deltas ever applied telescopes to the current render; rollback to any
//!   state is `diff(new_render, last_render)`.
//! - **DAG**: projections may consume other projections' outputs; the
//!   engine executes them in topological order within one derive pass.
//!
//! Layering: `sql-engine ← database ← database-projection ←
//! database-reactive`. The `kernel` modules ([`spec`], [`diff`],
//! [`engine`]) are database-free and programmed against [`RowReader`] /
//! [`ProjectionHost`]; [`db_host`] adapts the real [`database::Database`].

mod diff;
mod engine;
mod spec;

pub mod db_host;
pub mod typed;

pub use diff::multiset_diff;
pub use engine::{DeriveFailure, DeriveOutcome, ProjectionEngine, RegisterError};
pub use spec::{
    FoldCache, Inputs, OutputRow, OwnershipViolation, PartitionedSource, Projection,
    ProjectionHost, ProjectionSpec, ReadCtx, RowReader,
};
pub use typed::{Out, RenderCtx};
