//! The projection engine: registration (ownership + DAG validation) and
//! the derive pass that keeps derived tables in sync with their sources.

use std::collections::{HashMap, HashSet};

use sql_engine::execute::Params;
use sql_engine::reactive::execute::on_zset;
use sql_engine::reactive::registry::SubscriptionRegistry;
use sql_engine::reactive::SubscriptionId;
use sql_engine::storage::{CellValue, ZSet};

use crate::demand::{compile_footprint, display_name, InstanceName};
use crate::diff::multiset_diff;
use crate::spec::{
    FoldCache, Inputs, Lifecycle, OutputRow, OwnershipViolation, Projection, ProjectionHost,
    ProjectionSpec, ReadCtx,
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
    /// An on-demand projection's output would be consumed as a source/read —
    /// on-demand outputs are DAG leaves (v1, §12). `projection` is the
    /// projection owning the output.
    DemandOutputConsumed { projection: String, table: String },
    /// A data-presence spec whose source does not bind exactly its
    /// partition column to key component 0 — that shape IS the partition
    /// contract (§9); compound keys need [`Lifecycle::OnDemand`].
    InvalidPartitionBind { projection: String, table: String },
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
            RegisterError::DemandOutputConsumed { projection, table } => write!(
                f,
                "on-demand output table '{table}' of '{projection}' may not be consumed — \
                 on-demand projections are DAG leaves"
            ),
            RegisterError::InvalidPartitionBind { projection, table } => write!(
                f,
                "data-presence projection '{projection}': source '{table}' must bind \
                 exactly one column to key component 0 (the partition) — compound \
                 keys need Lifecycle::OnDemand"
            ),
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

/// Bookkeeping of one ACTIVE on-demand instance (§12). Exists from the
/// first `activate` to the `deactivate` that drops the refcount to 0.
struct InstanceState {
    refcount: u32,
    /// Registration in the engine's own `instance_registry`.
    sub_id: SubscriptionId,
    /// Execution memo, same contract as the static per-partition memo.
    cache: FoldCache,
    /// Rows of the last applied render.
    last_render: Vec<OutputRow>,
}

/// Registry + per-partition bookkeeping + the derive pass.
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

    // ── On-demand projections & their instances (§12) ────────────────
    demand_nodes: Vec<Node>,
    /// Output table → owning demand node index.
    demand_owner_by_table: HashMap<String, usize>,
    /// Read table → demand node indices (coarse re-render trigger).
    demand_reads_by_table: HashMap<String, Vec<usize>>,
    /// The engine's OWN registry for instance footprints — identification
    /// shares the candidates→verify machinery with query subscriptions,
    /// but the registry instance (and thus the id namespace) is private.
    instance_registry: SubscriptionRegistry,
    /// Registry id → (demand node index, instance name).
    sub_to_instance: HashMap<SubscriptionId, (usize, InstanceName)>,
    /// Per demand node: name → state of the active instance.
    instances: Vec<HashMap<InstanceName, InstanceState>>,
}

impl Default for ProjectionEngine {
    fn default() -> Self {
        Self {
            nodes: Vec::new(),
            owner_by_table: HashMap::new(),
            sources_by_table: HashMap::new(),
            reads_by_table: HashMap::new(),
            topo: Vec::new(),
            last_render: Vec::new(),
            live_partitions: Vec::new(),
            fold_caches: Vec::new(),
            demand_nodes: Vec::new(),
            demand_owner_by_table: HashMap::new(),
            demand_reads_by_table: HashMap::new(),
            instance_registry: SubscriptionRegistry::new(),
            sub_to_instance: HashMap::new(),
            instances: Vec::new(),
        }
    }
}

impl ProjectionEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty() && self.demand_nodes.is_empty()
    }

    pub fn projection_ids(&self) -> impl Iterator<Item = &str> {
        self.nodes
            .iter()
            .map(|n| n.spec.id.as_str())
            .chain(self.demand_nodes.iter().map(|n| n.spec.id.as_str()))
    }

    /// Tables owned by any projection (exclusively engine-written).
    pub fn owned_tables(&self) -> impl Iterator<Item = &str> {
        self.owner_by_table
            .keys()
            .map(|t| t.as_str())
            .chain(self.demand_owner_by_table.keys().map(|t| t.as_str()))
    }

    /// Register a projection; its spec's [`Lifecycle`] decides the
    /// machinery. Validation runs BEFORE any mutation, both ways:
    /// id uniqueness, output ownership, own-input use, then
    ///
    /// - [`Lifecycle::DataPresence`]: partition bind shape, graph
    ///   acyclicity (the projection joins the topo DAG), and the leaf
    ///   rule — it may not consume an on-demand projection's output.
    /// - [`Lifecycle::OnDemand`]: outputs are DAG leaves — nothing may
    ///   consume them (either direction). Its sources MAY be
    ///   data-presence outputs; instances run after the topo pass over
    ///   the accumulated delta (§12).
    pub fn register(&mut self, imp: Box<dyn Projection>) -> Result<(), RegisterError> {
        let spec = imp.spec();

        if self.nodes.iter().any(|n| n.spec.id == spec.id)
            || self.demand_nodes.iter().any(|n| n.spec.id == spec.id)
        {
            return Err(RegisterError::DuplicateId(spec.id));
        }
        for out in &spec.outputs {
            if let Some(&owner) = self.owner_by_table.get(out) {
                return Err(RegisterError::OutputAlreadyOwned {
                    table: out.clone(),
                    owner: self.nodes[owner].spec.id.clone(),
                });
            }
            if let Some(&owner) = self.demand_owner_by_table.get(out) {
                return Err(RegisterError::OutputAlreadyOwned {
                    table: out.clone(),
                    owner: self.demand_nodes[owner].spec.id.clone(),
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
        // Leaf rule (§12): nothing may consume an on-demand output — here
        // the new projection (either lifecycle) as the consumer.
        for input in spec.sources.iter().map(|s| &s.table).chain(spec.reads.iter()) {
            if let Some(&owner) = self.demand_owner_by_table.get(input) {
                return Err(RegisterError::DemandOutputConsumed {
                    projection: self.demand_nodes[owner].spec.id.clone(),
                    table: input.clone(),
                });
            }
        }

        match spec.lifecycle {
            Lifecycle::DataPresence => self.register_data_presence(spec, imp),
            Lifecycle::OnDemand => self.register_on_demand(spec, imp),
        }
    }

    fn register_data_presence(
        &mut self,
        spec: ProjectionSpec,
        imp: Box<dyn Projection>,
    ) -> Result<(), RegisterError> {
        for s in &spec.sources {
            if s.partition_column().is_none() {
                return Err(RegisterError::InvalidPartitionBind {
                    projection: spec.id.clone(),
                    table: s.table.clone(),
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
            let partition_column = s.partition_column().expect("validated above");
            self.sources_by_table
                .entry(s.table.clone())
                .or_default()
                .push((idx, partition_column));
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

    fn register_on_demand(
        &mut self,
        spec: ProjectionSpec,
        imp: Box<dyn Projection>,
    ) -> Result<(), RegisterError> {
        // Leaf rule, consumer side: no existing projection may already
        // consume the new projection's output tables.
        for out in &spec.outputs {
            let consumed = self
                .nodes
                .iter()
                .chain(self.demand_nodes.iter())
                .any(|n| {
                    n.spec.sources.iter().any(|s| &s.table == out)
                        || n.spec.reads.iter().any(|r| r == out)
                });
            if consumed {
                return Err(RegisterError::DemandOutputConsumed {
                    projection: spec.id.clone(),
                    table: out.clone(),
                });
            }
        }

        // Validated — commit.
        let idx = self.demand_nodes.len();
        for out in &spec.outputs {
            self.demand_owner_by_table.insert(out.clone(), idx);
        }
        for r in &spec.reads {
            self.demand_reads_by_table.entry(r.clone()).or_default().push(idx);
        }
        self.demand_nodes.push(Node { spec, imp });
        self.instances.push(HashMap::new());
        Ok(())
    }

    // ── On-demand instance lifecycle (§12) ───────────────────────────

    /// Activate the instance `(id, name)`: register its footprint in the
    /// instance registry and materialize it from the current local data.
    /// A second `activate` on a live instance only bumps its refcount and
    /// returns an empty outcome. An instance whose footprint matches zero
    /// rows stays ACTIVE with an empty render — demand is the lifecycle,
    /// not data presence.
    pub fn activate(
        &mut self,
        id: &str,
        name: &[CellValue],
        host: &mut dyn ProjectionHost,
    ) -> Result<DeriveOutcome, String> {
        let t = self
            .dyn_nodes
            .iter()
            .position(|n| n.spec.id == id)
            .ok_or_else(|| format!("unknown dynamic projection '{id}'"))?;

        if let Some(state) = self.instances[t].get_mut(name) {
            state.refcount += 1;
            return Ok(DeriveOutcome::default());
        }

        let conditions = compile_footprint(&self.dyn_nodes[t].spec, name)?;
        let sub_id = self
            .instance_registry
            .subscribe(&conditions, &[], &Params::new())
            .map_err(|e| format!("footprint registration failed: {e:?}"))?;
        self.sub_to_instance.insert(sub_id, (t, name.to_vec()));
        self.instances[t].insert(
            name.to_vec(),
            InstanceState {
                refcount: 1,
                sub_id,
                cache: FoldCache::default(),
                last_render: Vec::new(),
            },
        );

        let mut outcome = DeriveOutcome::default();
        self.recompute_instance_into(t, name, host, &mut outcome);
        Ok(outcome)
    }

    /// Release one activation of `(id, name)`. At refcount 0 the instance
    /// is evicted: footprint deregistered, output rows retracted, memo
    /// dropped. Deactivating an unknown instance is an error (a
    /// programming error of the embedder, not a runtime condition).
    pub fn deactivate(
        &mut self,
        id: &str,
        name: &[CellValue],
        host: &mut dyn ProjectionHost,
    ) -> Result<DeriveOutcome, String> {
        let t = self
            .dyn_nodes
            .iter()
            .position(|n| n.spec.id == id)
            .ok_or_else(|| format!("unknown dynamic projection '{id}'"))?;
        let state = self.instances[t]
            .get_mut(name)
            .ok_or_else(|| format!("instance '{}' of '{id}' is not active", display_name(name)))?;

        if state.refcount > 1 {
            state.refcount -= 1;
            return Ok(DeriveOutcome::default());
        }

        // Last reference: retract the render, then drop all state. On a
        // failed retraction the instance stays fully active.
        let retraction = multiset_diff(&[], &state.last_render);
        if !retraction.is_empty() {
            host.apply_delta(&retraction)?;
        }
        let state = self.instances[t].remove(name).expect("just found it");
        self.sub_to_instance.remove(&state.sub_id);
        self.instance_registry.unsubscribe(state.sub_id);

        Ok(DeriveOutcome {
            delta: retraction,
            // Clears a pinned failure of this instance at the embedder.
            succeeded: vec![(id.to_string(), display_name(name))],
            ..DeriveOutcome::default()
        })
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
            if let Some(&owner) = self.dyn_owner_by_table.get(&entry.table) {
                return Err(OwnershipViolation {
                    table: entry.table.clone(),
                    owner: self.dyn_nodes[owner].spec.id.clone(),
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
        if batch.is_empty() || (n == 0 && self.sub_to_instance.is_empty()) {
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

        // Dynamic pass (§12): runs AFTER the static topo loop so instance
        // footprints see the accumulated delta (external + derived) —
        // static outputs may be dynamic sources. Leaf rule: dynamic deltas
        // cascade nowhere, so one pass suffices.
        self.dynamic_pass(batch, &mut outcome, host);
        outcome
    }

    /// Identify affected instances via the instance registry, recompute
    /// them in deterministic order, append their deltas to the outcome.
    fn dynamic_pass(
        &mut self,
        batch: &ZSet,
        outcome: &mut DeriveOutcome,
        host: &mut dyn ProjectionHost,
    ) {
        // Guard: `on_zset` builds tracing spans (a row clone per mutation)
        // even against an empty registry — without active instances the
        // dynamic path must cost one branch, nothing more.
        if self.sub_to_instance.is_empty() {
            return;
        }

        let mut affected: HashSet<(usize, InstanceName)> = HashSet::new();
        for zset in [batch, &outcome.delta] {
            if zset.is_empty() {
                continue;
            }
            for sub_id in on_zset(&self.instance_registry, zset).keys() {
                if let Some(inst) = self.sub_to_instance.get(sub_id) {
                    affected.insert(inst.clone());
                }
            }
            // Read tables are coarse (like the static path): a hit
            // re-renders every active instance of the template.
            for entry in &zset.entries {
                if let Some(list) = self.dyn_reads_by_table.get(&entry.table) {
                    for &t in list {
                        for name in self.instances[t].keys() {
                            affected.insert((t, name.clone()));
                        }
                    }
                }
            }
        }

        let mut ordered: Vec<(usize, InstanceName)> = affected.into_iter().collect();
        ordered.sort();
        for (t, name) in ordered {
            self.recompute_instance_into(t, &name, host, outcome);
        }
    }

    /// Recompute one instance and fold the result into an outcome —
    /// shared by `activate`, the dynamic pass and `reset_and_rederive`.
    fn recompute_instance_into(
        &mut self,
        t: usize,
        name: &[CellValue],
        host: &mut dyn ProjectionHost,
        outcome: &mut DeriveOutcome,
    ) {
        match self.recompute_instance(t, name, host) {
            Ok(delta) => {
                outcome
                    .succeeded
                    .push((self.dyn_nodes[t].spec.id.clone(), display_name(name)));
                if !delta.is_empty() {
                    outcome.delta.extend(delta);
                }
            }
            Err(message) => outcome.failures.push(DeriveFailure {
                projection: self.dyn_nodes[t].spec.id.clone(),
                partition: Some(display_name(name)),
                message,
            }),
        }
    }

    /// Recompute one active instance: gather its footprint rows, run the
    /// pure function with the instance memo, diff against `last_render`,
    /// apply the delta through the host. On error the previous output
    /// stays untouched (no partial render).
    fn recompute_instance(
        &mut self,
        t: usize,
        name: &[CellValue],
        host: &mut dyn ProjectionHost,
    ) -> Result<ZSet, String> {
        let (sources, reads, outputs) = {
            let spec = &self.dyn_nodes[t].spec;
            (spec.sources.clone(), spec.reads.clone(), spec.outputs.clone())
        };

        let mut tables = Vec::with_capacity(sources.len());
        let mut total = 0usize;
        for s in &sources {
            let keys: Vec<(usize, CellValue)> =
                s.bind.iter().map(|&(col, comp)| (col, name[comp].clone())).collect();
            let rows = host.rows_matching(&s.table, &keys);
            total += rows.len();
            tables.push((s.table.clone(), rows));
        }

        let new_render: Vec<OutputRow> = if total == 0 {
            // The instance stays active (demand, not data presence), but
            // the memo dies with the last source row — a later insert
            // re-folds from zero.
            if let Some(state) = self.instances[t].get_mut(name) {
                state.cache = FoldCache::default();
            }
            Vec::new()
        } else {
            let inputs = Inputs { tables };
            let ctx = ReadCtx { reader: &*host, allowed: &reads };
            let state = self.instances[t]
                .get_mut(name)
                .expect("recompute of an instance without state");
            let rendered = self.dyn_nodes[t].imp.project(name, &inputs, &ctx, &mut state.cache)?;
            for (table, _) in &rendered {
                if !outputs.iter().any(|o| o == table) {
                    return Err(format!("rendered into undeclared output table '{table}'"));
                }
            }
            rendered
        };

        let state = self.instances[t]
            .get_mut(name)
            .expect("recompute of an instance without state");
        let delta = multiset_diff(&new_render, &state.last_render);
        if !delta.is_empty() {
            host.apply_delta(&delta)?;
        }
        let state = self.instances[t]
            .get_mut(name)
            .expect("recompute of an instance without state");
        state.last_render = new_render;
        Ok(delta)
    }

    /// Drop all derived state and rebuild every output table from the
    /// current source contents. Used after wholesale data replacement
    /// (`replace_data`) where `last_render` no longer matches reality:
    /// clears the output tables entirely, then re-derives all partitions
    /// found in the sources.
    pub fn reset_and_rederive(&mut self, host: &mut dyn ProjectionHost) -> DeriveOutcome {
        let mut outcome = DeriveOutcome::default();

        // 1. Clear every owned table (whatever the replacement put there) —
        // static AND dynamic outputs.
        let mut owned: Vec<&String> = self
            .owner_by_table
            .keys()
            .chain(self.dyn_owner_by_table.keys())
            .collect();
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
        // Dynamic instances survive the swap (registrations + refcounts
        // live in the engine), but their memos and renders are stale.
        for states in &mut self.instances {
            for state in states.values_mut() {
                state.cache = FoldCache::default();
                state.last_render.clear();
            }
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

        // 3. Re-materialize every ACTIVE dynamic instance from the new
        // source contents (which include the freshly derived static
        // outputs — dynamic sources may be static outputs).
        let mut active: Vec<(usize, InstanceName)> = self
            .instances
            .iter()
            .enumerate()
            .flat_map(|(t, states)| states.keys().map(move |name| (t, name.clone())))
            .collect();
        active.sort();
        for (t, name) in active {
            self.recompute_instance_into(t, &name, host, &mut outcome);
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

/// Canonical hyphenated form (8-4-4-4-12).
pub(crate) fn format_uuid(bytes: &[u8; 16]) -> String {
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
