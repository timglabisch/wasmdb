//! The projection engine: registration (ownership + DAG validation) and
//! the derive pass that keeps derived tables in sync with their sources.

use std::collections::{HashMap, HashSet};

use sql_engine::storage::{CellValue, ZSet};

use crate::diff::multiset_diff;
use crate::spec::{
    FoldCache, Inputs, OutputRow, OwnershipViolation, Projection, ProjectionHost, ProjectionSpec,
    ReadCtx,
};

/// Registration-time errors. All of them are programming errors in the
/// registering product, not runtime conditions.
#[derive(Debug)]
pub enum RegisterError {
    DuplicateId(String),
    OutputAlreadyOwned { table: String, owner: String },
    /// An output of the projection is also one of its own sources/reads.
    OutputIsOwnInput { projection: String, table: String },
    /// The projection graph would contain a cycle (ids in no particular order).
    Cycle(Vec<String>),
}

impl std::fmt::Display for RegisterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegisterError::DuplicateId(id) => write!(f, "projection id '{id}' already registered"),
            RegisterError::OutputAlreadyOwned { table, owner } => {
                write!(f, "output table '{table}' is already owned by projection '{owner}'")
            }
            RegisterError::OutputIsOwnInput { projection, table } => {
                write!(f, "projection '{projection}' uses its own output table '{table}' as input")
            }
            RegisterError::Cycle(ids) => write!(f, "projection graph has a cycle among: {ids:?}"),
        }
    }
}

impl std::error::Error for RegisterError {}

/// One failed partition recomputation. The partition's previous output
/// stays in place (no partial render); the failure is surfaced to the
/// embedder.
#[derive(Debug, Clone)]
pub struct DeriveFailure {
    pub projection: String,
    /// Display form of the partition; `None` for failures not tied to one.
    pub partition: Option<String>,
    pub message: String,
}

/// Result of one derive pass.
#[derive(Debug, Default)]
pub struct DeriveOutcome {
    /// All derived deltas applied during the pass, in application order.
    /// The embedder extends the triggering batch with this so subscribers
    /// see ONE consistent notification.
    pub delta: ZSet,
    pub failures: Vec<DeriveFailure>,
    /// Every `(projection id, display partition)` whose recomputation
    /// succeeded in this pass — including empty-delta renders. Lets the
    /// embedder clear a previously reported failure for the same
    /// partition (the error state is pinned until it provably re-derives).
    pub succeeded: Vec<(String, String)>,
}

struct Node {
    spec: ProjectionSpec,
    imp: Box<dyn Projection>,
}

/// Registry + per-partition bookkeeping + the derive pass.
#[derive(Default)]
pub struct ProjectionEngine {
    nodes: Vec<Node>,
    /// Output table → owning node index.
    owner_by_table: HashMap<String, usize>,
    /// Source table → [(node index, partition column)].
    sources_by_table: HashMap<String, Vec<(usize, usize)>>,
    /// Read table → node indices (coarse re-render trigger).
    reads_by_table: HashMap<String, Vec<usize>>,
    /// Node indices in topological order (sources before consumers).
    topo: Vec<usize>,
    /// Per node: partition → rows of the last applied render.
    last_render: Vec<HashMap<CellValue, Vec<OutputRow>>>,
    /// Per node: partitions that currently have ≥1 source row. Distinct
    /// from `last_render` because a live partition may legitimately render
    /// zero rows and must still re-render when a read table changes.
    live_partitions: Vec<HashSet<CellValue>>,
    /// Per node: partition → execution memo handed to `project` (§9.3).
    /// Lifecycle mirrors `live_partitions`: dropped with the partition,
    /// cleared on `reset_and_rederive`.
    fold_caches: Vec<HashMap<CellValue, FoldCache>>,
}

impl ProjectionEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn projection_ids(&self) -> impl Iterator<Item = &str> {
        self.nodes.iter().map(|n| n.spec.id.as_str())
    }

    /// Tables owned by any projection (exclusively engine-written).
    pub fn owned_tables(&self) -> impl Iterator<Item = &str> {
        self.owner_by_table.keys().map(|t| t.as_str())
    }

    /// Register a projection. Validates id uniqueness, output ownership
    /// and graph acyclicity BEFORE mutating any state.
    pub fn register(&mut self, imp: Box<dyn Projection>) -> Result<(), RegisterError> {
        let spec = imp.spec();

        if self.nodes.iter().any(|n| n.spec.id == spec.id) {
            return Err(RegisterError::DuplicateId(spec.id));
        }
        for out in &spec.outputs {
            if let Some(&owner) = self.owner_by_table.get(out) {
                return Err(RegisterError::OutputAlreadyOwned {
                    table: out.clone(),
                    owner: self.nodes[owner].spec.id.clone(),
                });
            }
            let is_own_input = spec.sources.iter().any(|s| &s.table == out)
                || spec.reads.iter().any(|r| r == out);
            if is_own_input {
                return Err(RegisterError::OutputIsOwnInput {
                    projection: spec.id.clone(),
                    table: out.clone(),
                });
            }
        }

        let specs: Vec<&ProjectionSpec> = self
            .nodes
            .iter()
            .map(|n| &n.spec)
            .chain(std::iter::once(&spec))
            .collect();
        let topo = toposort(&specs).map_err(RegisterError::Cycle)?;

        // Validated — commit.
        let idx = self.nodes.len();
        for out in &spec.outputs {
            self.owner_by_table.insert(out.clone(), idx);
        }
        for s in &spec.sources {
            self.sources_by_table
                .entry(s.table.clone())
                .or_default()
                .push((idx, s.partition_column));
        }
        for r in &spec.reads {
            self.reads_by_table.entry(r.clone()).or_default().push(idx);
        }
        self.nodes.push(Node { spec, imp });
        self.last_render.push(HashMap::new());
        self.live_partitions.push(HashSet::new());
        self.fold_caches.push(HashMap::new());
        self.topo = topo;
        Ok(())
    }

    /// Reject external batches that touch owned tables. Call BEFORE
    /// applying an external batch — derived tables are written exclusively
    /// by the engine.
    pub fn guard_external(&self, batch: &ZSet) -> Result<(), OwnershipViolation> {
        for entry in &batch.entries {
            if let Some(&owner) = self.owner_by_table.get(&entry.table) {
                return Err(OwnershipViolation {
                    table: entry.table.clone(),
                    owner: self.nodes[owner].spec.id.clone(),
                });
            }
        }
        Ok(())
    }

    /// One derive pass. `batch` is the external change set that has ALREADY
    /// been applied to the host's storage. Recomputes every affected
    /// (projection, partition) in topological order, applies each delta
    /// through the host (so downstream projections read upstream outputs),
    /// and returns the combined derived delta.
    pub fn derive(&mut self, batch: &ZSet, host: &mut dyn ProjectionHost) -> DeriveOutcome {
        let n = self.nodes.len();
        let mut outcome = DeriveOutcome::default();
        if n == 0 || batch.is_empty() {
            return outcome;
        }

        let mut dirty: Vec<HashSet<CellValue>> = vec![HashSet::new(); n];
        let mut read_dirty: Vec<bool> = vec![false; n];
        self.collect_dirty(batch, &mut dirty, &mut read_dirty);

        let order = self.topo.clone();
        for p in order {
            if read_dirty[p] {
                dirty[p].extend(self.live_partitions[p].iter().cloned());
            }
            if dirty[p].is_empty() {
                continue;
            }
            let mut partitions: Vec<CellValue> = dirty[p].drain().collect();
            partitions.sort();
            for partition in partitions {
                match self.recompute(p, &partition, host) {
                    Ok(delta) => {
                        outcome
                            .succeeded
                            .push((self.nodes[p].spec.id.clone(), display_partition(&partition)));
                        if !delta.is_empty() {
                            // Cascade: derived deltas may dirty downstream
                            // projections (always later in topo order —
                            // registration guarantees forward edges only).
                            self.collect_dirty(&delta, &mut dirty, &mut read_dirty);
                            outcome.delta.extend(delta);
                        }
                    }
                    Err(message) => outcome.failures.push(DeriveFailure {
                        projection: self.nodes[p].spec.id.clone(),
                        partition: Some(display_partition(&partition)),
                        message,
                    }),
                }
            }
        }
        outcome
    }

    /// Drop all derived state and rebuild every output table from the
    /// current source contents. Used after wholesale data replacement
    /// (`replace_data`) where `last_render` no longer matches reality:
    /// clears the output tables entirely, then re-derives all partitions
    /// found in the sources.
    pub fn reset_and_rederive(&mut self, host: &mut dyn ProjectionHost) -> DeriveOutcome {
        let mut outcome = DeriveOutcome::default();

        // 1. Clear every owned table (whatever the replacement put there).
        let mut owned: Vec<&String> = self.owner_by_table.keys().collect();
        owned.sort();
        let mut teardown = ZSet::new();
        for table in owned {
            for row in host.all_rows(table) {
                teardown.delete(table.clone(), row);
            }
        }
        if !teardown.is_empty() {
            if let Err(message) = host.apply_delta(&teardown) {
                outcome.failures.push(DeriveFailure {
                    projection: "<reset>".into(),
                    partition: None,
                    message,
                });
                return outcome;
            }
            outcome.delta.extend(teardown);
        }
        for render in &mut self.last_render {
            render.clear();
        }
        for live in &mut self.live_partitions {
            live.clear();
        }
        // Replacement invalidates every memo: seq lists could match the
        // new reality by coincidence while the row contents differ.
        for caches in &mut self.fold_caches {
            caches.clear();
        }

        // 2. Re-derive every partition present in any source, in topo order.
        let order = self.topo.clone();
        for p in order {
            let mut partitions: Vec<CellValue> = {
                let mut set = HashSet::new();
                for s in &self.nodes[p].spec.sources {
                    for row in host.all_rows(&s.table) {
                        if let Some(k) = row.get(s.partition_column) {
                            set.insert(k.clone());
                        }
                    }
                }
                set.into_iter().collect()
            };
            partitions.sort();
            for partition in partitions {
                match self.recompute(p, &partition, host) {
                    Ok(delta) => {
                        outcome
                            .succeeded
                            .push((self.nodes[p].spec.id.clone(), display_partition(&partition)));
                        outcome.delta.extend(delta);
                    }
                    Err(message) => outcome.failures.push(DeriveFailure {
                        projection: self.nodes[p].spec.id.clone(),
                        partition: Some(display_partition(&partition)),
                        message,
                    }),
                }
            }
        }
        outcome
    }

    fn collect_dirty(
        &self,
        batch: &ZSet,
        dirty: &mut [HashSet<CellValue>],
        read_dirty: &mut [bool],
    ) {
        for entry in &batch.entries {
            if let Some(list) = self.sources_by_table.get(&entry.table) {
                for &(p, partition_column) in list {
                    if let Some(partition) = entry.row.get(partition_column) {
                        dirty[p].insert(partition.clone());
                    }
                }
            }
            if let Some(list) = self.reads_by_table.get(&entry.table) {
                for &p in list {
                    read_dirty[p] = true;
                }
            }
        }
    }

    /// Recompute one (projection, partition): gather inputs, run the pure
    /// function, diff against `last_render`, apply the delta through the
    /// host. On error the previous output stays untouched (no partial
    /// render).
    fn recompute(
        &mut self,
        p: usize,
        partition: &CellValue,
        host: &mut dyn ProjectionHost,
    ) -> Result<ZSet, String> {
        let (sources, reads, outputs) = {
            let spec = &self.nodes[p].spec;
            (spec.sources.clone(), spec.reads.clone(), spec.outputs.clone())
        };

        let mut tables = Vec::with_capacity(sources.len());
        let mut total = 0usize;
        for s in &sources {
            let rows = host.rows_for_partition(&s.table, s.partition_column, partition);
            total += rows.len();
            tables.push((s.table.clone(), rows));
        }

        let new_render: Vec<OutputRow> = if total == 0 {
            // Data presence is the lifecycle: no source rows → no output,
            // and the pure function is not called. The execution memo
            // dies with the partition.
            self.live_partitions[p].remove(partition);
            self.fold_caches[p].remove(partition);
            Vec::new()
        } else {
            self.live_partitions[p].insert(partition.clone());
            let inputs = Inputs { tables };
            let ctx = ReadCtx { reader: &*host, allowed: &reads };
            let cache = self.fold_caches[p].entry(partition.clone()).or_default();
            let rendered = self.nodes[p].imp.project(partition, &inputs, &ctx, cache)?;
            for (table, _) in &rendered {
                if !outputs.iter().any(|o| o == table) {
                    return Err(format!("rendered into undeclared output table '{table}'"));
                }
            }
            rendered
        };

        let old = self.last_render[p].get(partition).cloned().unwrap_or_default();
        let delta = multiset_diff(&new_render, &old);
        if !delta.is_empty() {
            host.apply_delta(&delta)?;
        }
        if new_render.is_empty() {
            self.last_render[p].remove(partition);
        } else {
            self.last_render[p].insert(partition.clone(), new_render);
        }
        Ok(delta)
    }
}

fn display_partition(partition: &CellValue) -> String {
    match partition {
        CellValue::I64(v) => v.to_string(),
        CellValue::Str(s) => s.clone(),
        CellValue::Uuid(b) => format_uuid(b),
        CellValue::Null => "NULL".to_string(),
    }
}

/// Canonical hyphenated form (8-4-4-4-12) without pulling sql-parser in.
fn format_uuid(bytes: &[u8; 16]) -> String {
    let h: Vec<String> = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}{}{}{}-{}{}-{}{}-{}{}-{}{}{}{}{}{}",
        h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7], h[8], h[9], h[10], h[11], h[12], h[13],
        h[14], h[15]
    )
}

/// Kahn topological sort over the projection graph. Edge p→q iff an output
/// of p is a source or read of q. Returns node indices, sources first;
/// `Err` carries the ids stuck in a cycle.
fn toposort(specs: &[&ProjectionSpec]) -> Result<Vec<usize>, Vec<String>> {
    let n = specs.len();
    let mut successors: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut indegree = vec![0usize; n];

    for (p, sp) in specs.iter().enumerate() {
        for (q, sq) in specs.iter().enumerate() {
            if p == q {
                continue;
            }
            let feeds = sp.outputs.iter().any(|out| {
                sq.sources.iter().any(|s| &s.table == out) || sq.reads.iter().any(|r| r == out)
            });
            if feeds {
                successors[p].push(q);
                indegree[q] += 1;
            }
        }
    }

    let mut ready: Vec<usize> = (0..n).filter(|&i| indegree[i] == 0).collect();
    ready.sort();
    let mut order = Vec::with_capacity(n);
    while let Some(p) = ready.pop() {
        order.push(p);
        for &q in &successors[p] {
            indegree[q] -= 1;
            if indegree[q] == 0 {
                ready.push(q);
            }
        }
        ready.sort();
    }

    if order.len() == n {
        Ok(order)
    } else {
        let stuck: Vec<String> = (0..n)
            .filter(|&i| indegree[i] > 0)
            .map(|i| specs[i].id.clone())
            .collect();
        Err(stuck)
    }
}
