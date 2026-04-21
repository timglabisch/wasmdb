//! Re-export of the engine-level caller types at the database API boundary.
//!
//! [`Caller`] and [`CallerRegistry`] are defined in `sql-engine` because
//! both halves they bundle (`RequirementMeta`, `AsyncFetcherFn`) are
//! engine types. The database crate just exposes them at its own
//! top-level so users don't need to reach into `sql_engine` to construct
//! one.

pub use sql_engine::{Caller, CallerRegistry};
