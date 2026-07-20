//! Kernel tests for demand-driven instances (§12) against an in-memory
//! fake host. Cover the demand lifecycle (activate/refcount/deactivate),
//! composite-name routing via the instance registry, the static→dynamic
//! cascade within one pass, `reset_and_rederive` survival and failure
//! isolation.

use std::cell::Cell;
use std::collections::HashMap;
use std::rc::Rc;

use database_projection::{
    DynamicProjection, DynamicSpec, FoldCache, FootprintSource, Inputs, OutputRow,
    PartitionedSource, Projection, ProjectionEngine, ProjectionHost, ProjectionSpec, ReadCtx,
    RegisterError, RowReader,
};
use sql_engine::storage::{CellValue, ZSet};

// ── Fake host (multiset tables, like tests/kernel.rs) ────────────────────

#[derive(Default)]
struct FakeHost {
    tables: HashMap<String, Vec<Vec<CellValue>>>,
}

impl FakeHost {
    fn insert(&mut self, table: &str, row: Vec<CellValue>) -> ZSet {
        self.tables.entry(table.into()).or_default().push(row.clone());
        let mut z = ZSet::new();
        z.insert(table.into(), row);
        z
    }

    fn rows(&self, table: &str) -> Vec<Vec<CellValue>> {
        self.tables.get(table).cloned().unwrap_or_default()
    }
}

impl RowReader for FakeHost {
    fn rows_for_partition(&self, table: &str, partition_column: usize, key: &CellValue) -> Vec<Vec<CellValue>> {
        self.rows(table)
            .into_iter()
            .filter(|r| r.get(partition_column) == Some(key))
            .collect()
    }

    fn all_rows(&self, table: &str) -> Vec<Vec<CellValue>> {
        self.rows(table)
    }
}

impl ProjectionHost for FakeHost {
    fn apply_delta(&mut self, delta: &ZSet) -> Result<(), String> {
        for e in &delta.entries {
            let rows = self.tables.entry(e.table.clone()).or_default();
            if e.weight > 0 {
                rows.push(e.row.clone());
            } else if let Some(pos) = rows.iter().position(|r| *r == e.row) {
                rows.remove(pos);
            } else {
                return Err(format!("delete of missing row in '{}'", e.table));
            }
        }
        Ok(())
    }
}

fn i64v(v: i64) -> CellValue {
    CellValue::I64(v)
}

fn s(v: &str) -> CellValue {
    CellValue::Str(v.into())
}

// ── Fixture templates ────────────────────────────────────────────────────

/// events(kind, account, amount) → activity(account, entries, sum), bound
/// by name component 1 on the account column. Counts its `project` calls
/// so tests can assert on routing precision (registry miss = no call).
struct ActivityTemplate {
    calls: Rc<Cell<usize>>,
}

impl DynamicProjection for ActivityTemplate {
    fn spec(&self) -> DynamicSpec {
        DynamicSpec {
            id: "activity".into(),
            sources: vec![FootprintSource { table: "events".into(), bind: vec![(1, 1)] }],
            reads: vec![],
            outputs: vec!["activity".into()],
        }
    }

    fn project(
        &self,
        name: &[CellValue],
        inputs: &Inputs,
        _ctx: &ReadCtx<'_>,
        _cache: &mut FoldCache,
    ) -> Result<Vec<OutputRow>, String> {
        self.calls.set(self.calls.get() + 1);
        let rows = inputs.rows("events");
        let mut sum = 0i64;
        for r in rows {
            let CellValue::I64(v) = r[2] else { return Err("amount must be I64".into()) };
            sum += v;
        }
        Ok(vec![(
            "activity".to_string(),
            vec![name[1].clone(), i64v(rows.len() as i64), i64v(sum)],
        )])
    }
}

fn activity(calls: &Rc<Cell<usize>>) -> Box<ActivityTemplate> {
    Box::new(ActivityTemplate { calls: Rc::clone(calls) })
}

/// docs(tenant, doc, val) → doc_detail(tenant, doc, sum) with TWO bindings:
/// row matches iff tenant == name[0] AND doc == name[1].
struct DocDetailTemplate;

impl DynamicProjection for DocDetailTemplate {
    fn spec(&self) -> DynamicSpec {
        DynamicSpec {
            id: "doc_detail".into(),
            sources: vec![FootprintSource { table: "docs".into(), bind: vec![(0, 0), (1, 1)] }],
            reads: vec![],
            outputs: vec!["doc_detail".into()],
        }
    }

    fn project(
        &self,
        name: &[CellValue],
        inputs: &Inputs,
        _ctx: &ReadCtx<'_>,
        _cache: &mut FoldCache,
    ) -> Result<Vec<OutputRow>, String> {
        let mut sum = 0i64;
        for r in inputs.rows("docs") {
            let CellValue::I64(v) = r[2] else { return Err("val must be I64".into()) };
            sum += v;
        }
        Ok(vec![("doc_detail".to_string(), vec![name[0].clone(), name[1].clone(), i64v(sum)])])
    }
}

/// Static: events(kind, account, amount) → totals(account, sum),
/// partitioned by account (col 1).
struct TotalsProjection;

impl Projection for TotalsProjection {
    fn spec(&self) -> ProjectionSpec {
        ProjectionSpec {
            id: "totals".into(),
            sources: vec![PartitionedSource { table: "events".into(), partition_column: 1 }],
            reads: vec![],
            outputs: vec!["totals".into()],
        }
    }

    fn project(
        &self,
        key: &CellValue,
        inputs: &Inputs,
        _ctx: &ReadCtx<'_>,
        _cache: &mut FoldCache,
    ) -> Result<Vec<OutputRow>, String> {
        let mut sum = 0i64;
        for r in inputs.rows("events") {
            let CellValue::I64(v) = r[2] else { return Err("amount must be I64".into()) };
            sum += v;
        }
        Ok(vec![("totals".to_string(), vec![key.clone(), i64v(sum)])])
    }
}

/// Dynamic on a STATIC output: totals(account, sum) → total_watch(account,
/// doubled) — exercises the static→dynamic cascade in one derive pass.
struct TotalWatchTemplate;

impl DynamicProjection for TotalWatchTemplate {
    fn spec(&self) -> DynamicSpec {
        DynamicSpec {
            id: "total_watch".into(),
            sources: vec![FootprintSource { table: "totals".into(), bind: vec![(0, 1)] }],
            reads: vec![],
            outputs: vec!["total_watch".into()],
        }
    }

    fn project(
        &self,
        name: &[CellValue],
        inputs: &Inputs,
        _ctx: &ReadCtx<'_>,
        _cache: &mut FoldCache,
    ) -> Result<Vec<OutputRow>, String> {
        let mut out = Vec::new();
        for r in inputs.rows("totals") {
            let CellValue::I64(v) = r[1] else { return Err("sum must be I64".into()) };
            out.push(("total_watch".to_string(), vec![name[1].clone(), i64v(v * 2)]));
        }
        Ok(out)
    }
}

/// Fails on demand: amount == 13 is an error. For failure isolation.
struct FragileTemplate;

impl DynamicProjection for FragileTemplate {
    fn spec(&self) -> DynamicSpec {
        DynamicSpec {
            id: "fragile".into(),
            sources: vec![FootprintSource { table: "events".into(), bind: vec![(1, 1)] }],
            reads: vec![],
            outputs: vec!["fragile_out".into()],
        }
    }

    fn project(
        &self,
        name: &[CellValue],
        inputs: &Inputs,
        _ctx: &ReadCtx<'_>,
        _cache: &mut FoldCache,
    ) -> Result<Vec<OutputRow>, String> {
        let mut sum = 0i64;
        for r in inputs.rows("events") {
            let CellValue::I64(v) = r[2] else { return Err("amount must be I64".into()) };
            if v == 13 {
                return Err("unlucky amount".into());
            }
            sum += v;
        }
        Ok(vec![("fragile_out".to_string(), vec![name[1].clone(), i64v(sum)])])
    }
}

fn name_of(account: &str) -> Vec<CellValue> {
    vec![s("account"), s(account)]
}

fn seed_events(host: &mut FakeHost) {
    host.insert("events", vec![s("post"), s("alice"), i64v(10)]);
    host.insert("events", vec![s("post"), s("bob"), i64v(20)]);
    host.insert("events", vec![s("post"), s("carol"), i64v(30)]);
    host.insert("events", vec![s("post"), s("carol"), i64v(5)]);
}

// ── (a) activate materializes exactly the named instance ────────────────

#[test]
fn activate_materializes_only_the_named_instance() {
    let calls = Rc::new(Cell::new(0));
    let mut engine = ProjectionEngine::new();
    engine.register_dynamic(activity(&calls)).unwrap();
    let mut host = FakeHost::default();
    seed_events(&mut host);

    let outcome = engine.activate("activity", &name_of("carol"), &mut host).unwrap();
    assert!(outcome.failures.is_empty(), "{:?}", outcome.failures);
    assert_eq!(host.rows("activity"), vec![vec![s("carol"), i64v(2), i64v(35)]]);
    // Exactly one fold ran — alice/bob were not materialized.
    assert_eq!(calls.get(), 1);
    assert_eq!(outcome.succeeded, vec![("activity".to_string(), "account/carol".to_string())]);
}

// ── (b) compound name with two bindings: BOTH must match ────────────────

#[test]
fn two_bindings_require_both_components_to_match() {
    let mut engine = ProjectionEngine::new();
    engine.register_dynamic(Box::new(DocDetailTemplate)).unwrap();
    let mut host = FakeHost::default();
    host.insert("docs", vec![s("acme"), i64v(1), i64v(100)]);
    host.insert("docs", vec![s("acme"), i64v(2), i64v(200)]);
    host.insert("docs", vec![s("globex"), i64v(1), i64v(400)]);

    let name = vec![s("acme"), i64v(1)];
    engine.activate("doc_detail", &name, &mut host).unwrap();
    // Only (acme, 1) is gathered — same tenant/other doc and other
    // tenant/same doc stay out.
    assert_eq!(host.rows("doc_detail"), vec![vec![s("acme"), i64v(1), i64v(100)]]);

    // A row matching only ONE component must not re-route the instance.
    let z = host.insert("docs", vec![s("acme"), i64v(2), i64v(50)]);
    let outcome = engine.derive(&z, &mut host);
    assert!(outcome.delta.is_empty());

    // A row matching BOTH components updates it.
    let z = host.insert("docs", vec![s("acme"), i64v(1), i64v(7)]);
    let outcome = engine.derive(&z, &mut host);
    assert!(!outcome.delta.is_empty());
    assert_eq!(host.rows("doc_detail"), vec![vec![s("acme"), i64v(1), i64v(107)]]);
}

// ── (c) routing: matching row updates, non-matching is a registry miss ──

#[test]
fn routing_miss_does_not_recompute() {
    let calls = Rc::new(Cell::new(0));
    let mut engine = ProjectionEngine::new();
    engine.register_dynamic(activity(&calls)).unwrap();
    let mut host = FakeHost::default();
    seed_events(&mut host);

    engine.activate("activity", &name_of("carol"), &mut host).unwrap();
    assert_eq!(calls.get(), 1);

    // Non-matching account: registry miss, the fold must NOT run.
    let z = host.insert("events", vec![s("post"), s("alice"), i64v(99)]);
    let outcome = engine.derive(&z, &mut host);
    assert!(outcome.delta.is_empty());
    assert_eq!(calls.get(), 1);

    // Matching account: instance updates.
    let z = host.insert("events", vec![s("post"), s("carol"), i64v(1)]);
    let outcome = engine.derive(&z, &mut host);
    assert!(!outcome.delta.is_empty());
    assert_eq!(calls.get(), 2);
    assert_eq!(host.rows("activity"), vec![vec![s("carol"), i64v(3), i64v(36)]]);
}

// ── (d) refcount: alive until the last deactivate, then retracted ───────

#[test]
fn refcount_keeps_instance_until_last_deactivate() {
    let calls = Rc::new(Cell::new(0));
    let mut engine = ProjectionEngine::new();
    engine.register_dynamic(activity(&calls)).unwrap();
    let mut host = FakeHost::default();
    seed_events(&mut host);

    engine.activate("activity", &name_of("carol"), &mut host).unwrap();
    let second = engine.activate("activity", &name_of("carol"), &mut host).unwrap();
    // Refcount bump: no work, no delta.
    assert!(second.delta.is_empty());
    assert_eq!(calls.get(), 1);

    let first_release = engine.deactivate("activity", &name_of("carol"), &mut host).unwrap();
    assert!(first_release.delta.is_empty());
    assert_eq!(host.rows("activity"), vec![vec![s("carol"), i64v(2), i64v(35)]]);

    let last_release = engine.deactivate("activity", &name_of("carol"), &mut host).unwrap();
    assert!(!last_release.delta.is_empty());
    assert!(host.rows("activity").is_empty());

    // Registry drained: a matching insert routes nowhere and folds nothing.
    let z = host.insert("events", vec![s("post"), s("carol"), i64v(1)]);
    let outcome = engine.derive(&z, &mut host);
    assert!(outcome.delta.is_empty());
    assert_eq!(calls.get(), 1);

    // Deactivating again is an embedder programming error.
    assert!(engine.deactivate("activity", &name_of("carol"), &mut host).is_err());
}

// ── (e) empty footprint: instance stays ACTIVE with an empty render ─────

#[test]
fn empty_footprint_stays_active_and_materializes_later() {
    let calls = Rc::new(Cell::new(0));
    let mut engine = ProjectionEngine::new();
    engine.register_dynamic(activity(&calls)).unwrap();
    let mut host = FakeHost::default();
    seed_events(&mut host);

    // "dave" has no rows — demand, not data presence: active, empty render,
    // fold not called (nothing to fold).
    let outcome = engine.activate("activity", &name_of("dave"), &mut host).unwrap();
    assert!(outcome.delta.is_empty());
    assert!(outcome.failures.is_empty());
    assert_eq!(calls.get(), 0);
    assert!(host.rows("activity").is_empty());

    // First matching insert materializes the already-active instance.
    let z = host.insert("events", vec![s("post"), s("dave"), i64v(42)]);
    let outcome = engine.derive(&z, &mut host);
    assert!(!outcome.delta.is_empty());
    assert_eq!(host.rows("activity"), vec![vec![s("dave"), i64v(1), i64v(42)]]);
}

// ── (f) static output as dynamic source: one pass, one delta ────────────

#[test]
fn static_output_feeds_dynamic_instance_in_one_pass() {
    let mut engine = ProjectionEngine::new();
    engine.register(Box::new(TotalsProjection)).unwrap();
    engine.register_dynamic(Box::new(TotalWatchTemplate)).unwrap();
    let mut host = FakeHost::default();
    let z = host.insert("events", vec![s("post"), s("carol"), i64v(30)]);
    engine.derive(&z, &mut host);
    assert_eq!(host.rows("totals"), vec![vec![s("carol"), i64v(30)]]);

    engine.activate("total_watch", &name_of("carol"), &mut host).unwrap();
    assert_eq!(host.rows("total_watch"), vec![vec![s("carol"), i64v(60)]]);

    // One external event → static totals recomputes AND the dynamic
    // instance follows, both inside ONE derive outcome.
    let z = host.insert("events", vec![s("post"), s("carol"), i64v(10)]);
    let outcome = engine.derive(&z, &mut host);
    assert_eq!(host.rows("totals"), vec![vec![s("carol"), i64v(40)]]);
    assert_eq!(host.rows("total_watch"), vec![vec![s("carol"), i64v(80)]]);
    let touches_watch = outcome.delta.entries.iter().any(|e| e.table == "total_watch");
    let touches_totals = outcome.delta.entries.iter().any(|e| e.table == "totals");
    assert!(touches_totals && touches_watch, "one combined delta expected");
}

// ── (g) reset_and_rederive keeps active instances ───────────────────────

#[test]
fn reset_and_rederive_rematerializes_active_instances() {
    let calls = Rc::new(Cell::new(0));
    let mut engine = ProjectionEngine::new();
    engine.register_dynamic(activity(&calls)).unwrap();
    let mut host = FakeHost::default();
    seed_events(&mut host);
    engine.activate("activity", &name_of("carol"), &mut host).unwrap();

    // Simulate wholesale replacement: swap the source contents underneath.
    host.tables.insert("events".into(), vec![vec![s("post"), s("carol"), i64v(500)]]);

    let outcome = engine.reset_and_rederive(&mut host);
    assert!(outcome.failures.is_empty(), "{:?}", outcome.failures);
    assert_eq!(host.rows("activity"), vec![vec![s("carol"), i64v(1), i64v(500)]]);

    // Still routed after the rebuild.
    let z = host.insert("events", vec![s("post"), s("carol"), i64v(1)]);
    engine.derive(&z, &mut host);
    assert_eq!(host.rows("activity"), vec![vec![s("carol"), i64v(2), i64v(501)]]);
}

// ── (h) fold failure: DeriveFailure with display name, old output stays ─

#[test]
fn fold_failure_keeps_previous_output() {
    let mut engine = ProjectionEngine::new();
    engine.register_dynamic(Box::new(FragileTemplate)).unwrap();
    let mut host = FakeHost::default();
    host.insert("events", vec![s("post"), s("carol"), i64v(30)]);

    engine.activate("fragile", &name_of("carol"), &mut host).unwrap();
    assert_eq!(host.rows("fragile_out"), vec![vec![s("carol"), i64v(30)]]);

    let z = host.insert("events", vec![s("post"), s("carol"), i64v(13)]);
    let outcome = engine.derive(&z, &mut host);
    assert_eq!(outcome.failures.len(), 1);
    assert_eq!(outcome.failures[0].projection, "fragile");
    assert_eq!(outcome.failures[0].partition.as_deref(), Some("account/carol"));
    // No partial render — the previous output survives the failure.
    assert_eq!(host.rows("fragile_out"), vec![vec![s("carol"), i64v(30)]]);
}

// ── Leaf rule: dynamic outputs may not be consumed ──────────────────────

#[test]
fn dynamic_outputs_are_leaves() {
    // Static consuming a dynamic output → rejected.
    let calls = Rc::new(Cell::new(0));
    let mut engine = ProjectionEngine::new();
    engine.register_dynamic(activity(&calls)).unwrap();

    struct EatsActivity;
    impl Projection for EatsActivity {
        fn spec(&self) -> ProjectionSpec {
            ProjectionSpec {
                id: "eats".into(),
                sources: vec![PartitionedSource { table: "activity".into(), partition_column: 0 }],
                reads: vec![],
                outputs: vec!["eats_out".into()],
            }
        }
        fn project(
            &self,
            _key: &CellValue,
            _inputs: &Inputs,
            _ctx: &ReadCtx<'_>,
            _cache: &mut FoldCache,
        ) -> Result<Vec<OutputRow>, String> {
            Ok(vec![])
        }
    }
    match engine.register(Box::new(EatsActivity)) {
        Err(RegisterError::DynamicOutputConsumed { projection, table }) => {
            assert_eq!(projection, "activity");
            assert_eq!(table, "activity");
        }
        other => panic!("expected DynamicOutputConsumed, got {other:?}"),
    }

    // Dynamic consuming a dynamic output → rejected as well.
    struct EatsActivityDyn;
    impl DynamicProjection for EatsActivityDyn {
        fn spec(&self) -> DynamicSpec {
            DynamicSpec {
                id: "eats_dyn".into(),
                sources: vec![FootprintSource { table: "activity".into(), bind: vec![(0, 0)] }],
                reads: vec![],
                outputs: vec!["eats_dyn_out".into()],
            }
        }
        fn project(
            &self,
            _name: &[CellValue],
            _inputs: &Inputs,
            _ctx: &ReadCtx<'_>,
            _cache: &mut FoldCache,
        ) -> Result<Vec<OutputRow>, String> {
            Ok(vec![])
        }
    }
    match engine.register_dynamic(Box::new(EatsActivityDyn)) {
        Err(RegisterError::DynamicOutputConsumed { .. }) => {}
        other => panic!("expected DynamicOutputConsumed, got {other:?}"),
    }
}
