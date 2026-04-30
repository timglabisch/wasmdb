//! The `DbRequirement` trait — the typed counterpart of [`Requirement`].
//!
//! Pure marker/meta trait. Codegen-emitted query markers implement it
//! to expose their wire id, row table, and parameter shape. The fetch
//! closure itself lives in [`RequirementRegistry`], built by codegen
//! with captured dependencies — not on this trait.
//!
//! [`Requirement`]: crate::registry::Requirement
//! [`RequirementRegistry`]: crate::registry::RequirementRegistry

use sql_engine::DbTable;

use crate::meta::RequirementMeta;

/// Query-marker → registrable requirement definition.
pub trait DbRequirement: 'static {
    /// Stable requirement id (wire form `"{schema}::{function}"`),
    /// matches [`Requirement::id`] and the registry key.
    ///
    /// [`Requirement::id`]: crate::registry::Requirement::id
    const ID: &'static str;

    /// Row type produced by this requirement — determines the `row_table`
    /// into which the runtime upserts results.
    type Row: DbTable;

    /// Static metadata (row_table + positional param shape). Must agree
    /// with the [`RequirementMeta`] stored under the same id in the
    /// [`RequirementRegistry`].
    ///
    /// [`RequirementRegistry`]: crate::registry::RequirementRegistry
    fn meta() -> RequirementMeta;
}
