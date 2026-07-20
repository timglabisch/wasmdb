//! The projection contract: what a product registers and what the engine
//! calls. Everything here is database-free; the engine talks to storage
//! through [`RowReader`] / [`ProjectionHost`].

use sql_engine::storage::{CellValue, ZSet};

/// One rendered output row: target table name + cells in schema order.
pub type OutputRow = (String, Vec<CellValue>);

/// One source table with the equality bindings that scope its rows to one
/// instance key: `(column index, key component index)` — a row belongs to
/// the instance iff `row[col] == key[comp]` for every binding.
///
/// A data-presence source ([`Lifecycle::DataPresence`]) binds exactly its
/// partition column to key component 0. An on-demand source may bind any
/// columns against any components of the compound name; an empty bind list
/// means every row belongs to every instance (table scan).
#[derive(Debug, Clone)]
pub struct ProjectionSource {
    pub table: String,
    pub bind: Vec<(usize, usize)>,
}

impl ProjectionSource {
    /// The data-presence shape: the partition column bound to key
    /// component 0.
    pub fn partitioned(table: impl Into<String>, partition_column: usize) -> Self {
        Self { table: table.into(), bind: vec![(partition_column, 0)] }
    }

    /// The partition column of a data-presence source — `Some(col)` iff
    /// the bind list is exactly `[(col, 0)]`. Registration enforces this
    /// shape for [`Lifecycle::DataPresence`] specs.
    pub fn partition_column(&self) -> Option<usize> {
        match self.bind.as_slice() {
            [(col, 0)] => Some(*col),
            _ => None,
        }
    }
}

/// What brings instances of a projection into existence (§9 vs §12). The
/// fold and render contract is identical in both — only the lifecycle of
/// the instances differs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lifecycle {
    /// One instance per partition value present in the source rows; the
    /// key has ONE component (the partition value). An instance appears
    /// with the first source row and disappears with the last.
    DataPresence,
    /// Instances are activated/deactivated by compound name at runtime
    /// (refcounted); zero source rows is a valid ACTIVE state with an
    /// empty render. Outputs are DAG leaves (v1, §12).
    OnDemand,
}

/// Static description of a projection, produced once at registration.
#[derive(Debug, Clone)]
pub struct ProjectionSpec {
    /// Unique id (used in errors, debug output and — later — slot identity).
    pub id: String,
    pub lifecycle: Lifecycle,
    /// The tables this projection derives FROM, with their key bindings.
    pub sources: Vec<ProjectionSource>,
    /// Read-only render inputs, available through [`ReadCtx`]. Any change
    /// to one of these re-renders ALL live instances (coarse by design — v1).
    pub reads: Vec<String>,
    /// Output tables. Owned exclusively by this projection; nothing else
    /// may write them.
    pub outputs: Vec<String>,
}

/// A materialized view defined as a Rust function.
///
/// `project` must be PURE: deterministic, no IO, no clock, no RNG, no
/// global state. It is called per instance with the current rows of
/// every source; its return value fully replaces the previous render of
/// that instance. `key` is the instance identity: `[partition]` (one
/// component) under [`Lifecycle::DataPresence`], the compound activation
/// name under [`Lifecycle::OnDemand`]. It is NOT called while the sources
/// hold zero rows for the instance — the engine renders empty; what
/// differs by lifecycle is whether the instance itself survives that
/// (demand: yes, data presence: no).
///
/// `cache` is an execution memo, never an input: the returned rows must
/// be a pure function of `(inputs, ctx)` alone — an empty cache must
/// always produce the same result, just with more work.
pub trait Projection {
    fn spec(&self) -> ProjectionSpec;

    fn project(
        &self,
        key: &[CellValue],
        inputs: &Inputs,
        ctx: &ReadCtx<'_>,
        cache: &mut FoldCache,
    ) -> Result<Vec<OutputRow>, String>;
}

/// Opaque per-(projection, partition) execution memo (§9.3). Owned by
/// the engine so its lifecycle follows the partition: dropped when the
/// partition loses its last source row, cleared wholesale on
/// `reset_and_rederive`. The fold shim memoizes its committed-prefix
/// state here; hand-written impls are free to ignore it.
#[derive(Default)]
pub struct FoldCache(Option<Box<dyn std::any::Any>>);

impl FoldCache {
    pub fn get<T: 'static>(&self) -> Option<&T> {
        self.0.as_ref()?.downcast_ref()
    }

    pub fn put<T: 'static>(&mut self, value: T) {
        self.0 = Some(Box::new(value));
    }
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

    /// All live rows of `table` whose cells equal `keys` at every listed
    /// column — the gather primitive of on-demand instances (§12). The
    /// default implementation scans; hosts with indexes should override
    /// (see `DatabaseHost`).
    fn rows_matching(&self, table: &str, keys: &[(usize, CellValue)]) -> Vec<Vec<CellValue>> {
        self.all_rows(table)
            .into_iter()
            .filter(|row| keys.iter().all(|(col, v)| row.get(*col) == Some(v)))
            .collect()
    }
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
