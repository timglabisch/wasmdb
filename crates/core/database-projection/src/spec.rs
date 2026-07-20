//! The projection contract: what a product registers and what the engine
//! calls. Everything here is database-free; the engine talks to storage
//! through [`RowReader`] / [`ProjectionHost`].

use sql_engine::storage::{CellValue, ZSet};

/// One rendered output row: target table name + cells in schema order.
pub type OutputRow = (String, Vec<CellValue>);

/// A partitioned source table. Changes to it dirty the partition values
/// extracted from `partition_column`; the rows of a partition are handed
/// to `project` as inputs.
#[derive(Debug, Clone)]
pub struct PartitionedSource {
    pub table: String,
    /// Column index of the partition within this table's rows.
    pub partition_column: usize,
}

/// Static description of a projection, produced once at registration.
#[derive(Debug, Clone)]
pub struct ProjectionSpec {
    /// Unique id (used in errors, debug output and — later — slot identity).
    pub id: String,
    /// Partitioned sources: the tables this projection derives FROM.
    pub sources: Vec<PartitionedSource>,
    /// Read-only render inputs, available through [`ReadCtx`]. Any change
    /// to one of these re-renders ALL live partitions (coarse by design — v1).
    pub reads: Vec<String>,
    /// Output tables. Owned exclusively by this projection; nothing else
    /// may write them.
    pub outputs: Vec<String>,
}

/// A materialized view defined as a Rust function.
///
/// `project` must be PURE: deterministic, no IO, no clock, no RNG, no
/// global state. It is called per partition with the current rows of
/// every source; its return value fully replaces the previous render of
/// that partition. It is NOT called for partitions whose sources hold
/// zero rows — data presence is the lifecycle (the engine clears the
/// partition's output).
pub trait Projection {
    fn spec(&self) -> ProjectionSpec;

    fn project(
        &self,
        partition: &CellValue,
        inputs: &Inputs,
        ctx: &ReadCtx<'_>,
    ) -> Result<Vec<OutputRow>, String>;
}

/// Rows of the sources for the partition being recomputed, in declaration
/// order of [`ProjectionSpec::sources`].
#[derive(Debug)]
pub struct Inputs {
    pub(crate) tables: Vec<(String, Vec<Vec<CellValue>>)>,
}

impl Inputs {
    /// Rows of the given source table (empty slice if the table holds no
    /// rows for this partition or is not a declared source).
    pub fn rows(&self, table: &str) -> &[Vec<CellValue>] {
        self.tables
            .iter()
            .find(|(t, _)| t == table)
            .map(|(_, rows)| rows.as_slice())
            .unwrap_or(&[])
    }

    /// Total row count across all sources.
    pub fn total_rows(&self) -> usize {
        self.tables.iter().map(|(_, rows)| rows.len()).sum()
    }
}

/// Read-only access to the tables declared in [`ProjectionSpec::reads`].
/// Reading an undeclared table is an error — the declaration is what makes
/// the reactive re-render on those tables sound.
pub struct ReadCtx<'a> {
    pub(crate) reader: &'a dyn ProjectionHost,
    pub(crate) allowed: &'a [String],
}

impl ReadCtx<'_> {
    /// All rows of a declared read table.
    pub fn rows(&self, table: &str) -> Result<Vec<Vec<CellValue>>, String> {
        if !self.allowed.iter().any(|t| t == table) {
            return Err(format!("table '{table}' is not declared in reads"));
        }
        Ok(self.reader.all_rows(table))
    }
}

/// Read side of the storage the engine derives over.
pub trait RowReader {
    /// All live rows of `table` whose cell at `partition_column` equals
    /// `partition`.
    fn rows_for_partition(
        &self,
        table: &str,
        partition_column: usize,
        partition: &CellValue,
    ) -> Vec<Vec<CellValue>>;

    /// All live rows of `table`. Unknown tables yield an empty vec.
    fn all_rows(&self, table: &str) -> Vec<Vec<CellValue>>;
}

/// Full storage host: read access plus delta application. The engine
/// applies derived deltas through this so downstream projections in the
/// same derive pass observe upstream outputs.
pub trait ProjectionHost: RowReader {
    fn apply_delta(&mut self, delta: &ZSet) -> Result<(), String>;
}

/// An external batch tried to write a table owned by a projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipViolation {
    pub table: String,
    /// Id of the owning projection.
    pub owner: String,
}

impl std::fmt::Display for OwnershipViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "table '{}' is owned by projection '{}' — external writes are not allowed",
            self.table, self.owner
        )
    }
}

impl std::error::Error for OwnershipViolation {}
