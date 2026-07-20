//! Typed façade over the row-level projection contract.
//!
//! Product projections written with `#[projection]` (tables-macros) get
//! their source rows decoded into `#[row]` structs, read tables through a
//! typed [`RenderCtx`], and emit output rows through [`Out`] — all built
//! on `DbTable`'s cell round-trip (`into_cells` / `from_cells`). Nothing
//! here is macro-only: hand-written [`Projection`](crate::Projection)
//! impls can use the same pieces.

use sql_engine::storage::CellValue;
use sql_engine::DbTable;

use crate::spec::{OutputRow, ReadCtx};

/// Decode raw storage rows into typed `#[row]` structs.
pub fn decode_rows<R: DbTable>(rows: &[Vec<CellValue>]) -> Result<Vec<R>, String> {
    rows.iter().map(|r| R::from_cells(r)).collect()
}

/// Resolve the column index of the partition column in `R`'s schema.
/// Called once per source at registration time (inside `ProjectionSpec`
/// construction); a missing column is a wiring error in the registering
/// product, so it panics with a pointed message rather than surfacing
/// per-derive.
pub fn partition_column_index<R: DbTable>(partition: &str) -> usize {
    R::schema()
        .columns
        .iter()
        .position(|c| c.name == partition)
        .unwrap_or_else(|| {
            panic!(
                "projection source table '{}' has no partition column '{partition}'",
                R::TABLE
            )
        })
}

/// Typed view of the declared read tables. Wraps [`ReadCtx`]; reading a
/// table not declared in `reads` is an error, same as the raw contract.
pub struct RenderCtx<'a> {
    inner: &'a ReadCtx<'a>,
}

impl<'a> RenderCtx<'a> {
    pub fn new(inner: &'a ReadCtx<'a>) -> Self {
        Self { inner }
    }

    /// All rows of the declared read table `R`, decoded.
    pub fn all<R: DbTable>(&self) -> Result<Vec<R>, String> {
        decode_rows(&self.inner.rows(R::TABLE)?)
    }
}

/// Typed output collector: `emit` a `#[row]` struct per derived row. The
/// target table comes from the row type; multiple output tables are just
/// multiple row types.
#[derive(Default)]
pub struct Out {
    rows: Vec<OutputRow>,
}

impl Out {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn emit<R: DbTable>(&mut self, row: R) {
        self.rows.push((R::TABLE.to_string(), row.into_cells()));
    }

    pub fn into_rows(self) -> Vec<OutputRow> {
        self.rows
    }
}
