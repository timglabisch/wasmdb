//! `DbTable` — Rust-Type ↔ row in a `Database` table.
//!
//! Provides the [`TableSchema`] used at `register_table` time plus a
//! cell projection (`into_cells`) in the schema's column order, so typed
//! rows can be fed into the engine's `Vec<CellValue>` storage without
//! hand-writing glue.
//!
//! Emitted by the `#[row]` proc-macro on the original struct and by
//! `tables-codegen` on the client-side duplicate. Both produce an impl
//! with identical semantics — server and client share the row shape.

use crate::schema::TableSchema;
use crate::storage::CellValue;

pub trait DbTable: Sized {
    /// Stable table name used as both `TableSchema::name` and the
    /// lookup key in `Database::tables`. Convention: `snake_case` of
    /// the Rust struct name.
    const TABLE: &'static str;

    /// Full schema for this row type. Callers register it via
    /// `Database::register_table::<Self>()`.
    fn schema() -> TableSchema;

    /// Project one instance into a cell row in `schema().columns`
    /// order.
    fn into_cells(self) -> Vec<CellValue>;
}
