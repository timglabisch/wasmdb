//! Kernel tests against an in-memory fake host — no `Database`, no
//! reactive layer. Cover the invariants from the design doc: telescoping
//! (`Σ deltas ≡ last_render`), key death, ownership, DAG cascade,
//! determinism, read-table re-render and failure isolation.

use std::collections::HashMap;

use database_projection::{
    FoldCache, Inputs, OutputRow, PartitionedSource, Projection, ProjectionEngine,
    ProjectionHost, ProjectionSpec, ReadCtx, RowReader,
};
use sql_engine::storage::{CellValue, ZSet};

// ── Fake host ────────────────────────────────────────────────────────────

/// Multiset table store: apply +1 pushes a row, −1 removes the first
/// equal row. Enough to verify engine invariants; PK semantics live in
/// the `DatabaseHost` adapter and are tested there.
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

    fn remove(&mut self, table: &str, row: Vec<CellValue>) -> ZSet {
        let rows = self.tables.entry(table.into()).or_default();
        if let Some(pos) = rows.iter().position(|r| *r == row) {
            rows.remove(pos);
        }
        let mut z = ZSet::new();
        z.delete(table.into(), row);
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

// ── Fixture projections ──────────────────────────────────────────────────

fn i64v(v: i64) -> CellValue {
    CellValue::I64(v)
}

/// events(doc_id, seq, val) → totals(doc_id, sum) + lines(doc_id, seq, val).
struct DraftProjection;

impl Projection for DraftProjection {
    fn spec(&self) -> ProjectionSpec {
        ProjectionSpec {
            id: "draft".into(),
            sources: vec![PartitionedSource { table: "events".into(), partition_column: 0 }],
            reads: vec![],
            outputs: vec!["totals".into(), "lines".into()],
        }
    }

    fn project(
        &self,
        key: &CellValue,
        inputs: &Inputs,
        _ctx: &ReadCtx<'_>,
        _cache: &mut FoldCache,
    ) -> Result<Vec<OutputRow>, String> {
        let mut events = inputs.rows("events").to_vec();
        events.sort_by_key(|r| r[1].clone());
        let mut out = Vec::new();
        let mut sum = 0i64;
        for e in &events {
            let CellValue::I64(v) = e[2] else {
                return Err("val must be I64".into());
            };
            sum += v;
            out.push(("lines".to_string(), vec![key.clone(), e[1].clone(), e[2].clone()]));
        }
        out.push(("totals".to_string(), vec![key.clone(), i64v(sum)]));
        Ok(out)
    }
}

/// totals(doc_id, sum) → doubled(doc_id, sum*2) — downstream of `draft`.
struct DoubledProjection;

impl Projection for DoubledProjection {
    fn spec(&self) -> ProjectionSpec {
        ProjectionSpec {
            id: "doubled".into(),
            sources: vec![PartitionedSource { table: "totals".into(), partition_column: 0 }],
            reads: vec![],
            outputs: vec!["doubled".into()],
        }
    }

    fn project(
        &self,
        key: &CellValue,
        inputs: &Inputs,
        _ctx: &ReadCtx<'_>,
        _cache: &mut FoldCache,
    ) -> Result<Vec<OutputRow>, String> {
        let mut out = Vec::new();
        for r in inputs.rows("totals") {
            let CellValue::I64(sum) = r[1] else {
                return Err("sum must be I64".into());
            };
            out.push(("doubled".to_string(), vec![key.clone(), i64v(sum * 2)]));
        }
        Ok(out)
    }
}

fn engine_with_draft() -> ProjectionEngine {
    let mut engine = ProjectionEngine::new();
    engine.register(Box::new(DraftProjection)).unwrap();
    engine
}

fn event(doc: i64, seq: i64, val: i64) -> Vec<CellValue> {
    vec![i64v(doc), i64v(seq), i64v(val)]
}

// ── Basic derivation ─────────────────────────────────────────────────────

#[test]
fn insert_derives_totals_and_lines() {
    let mut engine = engine_with_draft();
    let mut host = FakeHost::default();

    let batch = host.insert("events", event(1, 0, 100));
    let outcome = engine.derive(&batch, &mut host);
    assert!(outcome.failures.is_empty());

    assert_eq!(host.rows("totals"), vec![vec![i64v(1), i64v(100)]]);
    assert_eq!(host.rows("lines"), vec![vec![i64v(1), i64v(0), i64v(100)]]);

    let batch = host.insert("events", event(1, 1, 50));
    let outcome = engine.derive(&batch, &mut host);
    assert!(outcome.failures.is_empty());
    assert_eq!(host.rows("totals"), vec![vec![i64v(1), i64v(150)]]);
    assert_eq!(host.rows("lines").len(), 2);
}

#[test]
fn unrelated_key_untouched() {
    let mut engine = engine_with_draft();
    let mut host = FakeHost::default();

    let b = host.insert("events", event(1, 0, 100));
    engine.derive(&b, &mut host);
    let b = host.insert("events", event(2, 0, 7));
    let outcome = engine.derive(&b, &mut host);

    // Doc 1's output must not appear in the delta for doc 2's change.
    assert!(outcome
        .delta
        .entries
        .iter()
        .all(|e| e.row[0] == i64v(2)));
    let mut totals = host.rows("totals");
    totals.sort();
    assert_eq!(totals, vec![vec![i64v(1), i64v(100)], vec![i64v(2), i64v(7)]]);
}

#[test]
fn rederive_without_change_is_empty_delta() {
    let mut engine = engine_with_draft();
    let mut host = FakeHost::default();

    let batch = host.insert("events", event(1, 0, 100));
    engine.derive(&batch, &mut host);
    // Same batch again: sources unchanged relative to output → empty delta.
    let outcome = engine.derive(&batch, &mut host);
    assert!(outcome.delta.is_empty());
    assert!(outcome.failures.is_empty());
}

// ── Reject flow / key death ──────────────────────────────────────────────

#[test]
fn removing_source_rows_rederives() {
    let mut engine = engine_with_draft();
    let mut host = FakeHost::default();

    let b = host.insert("events", event(1, 0, 100));
    engine.derive(&b, &mut host);
    let b = host.insert("events", event(1, 1, 50));
    engine.derive(&b, &mut host);

    // Invert of the second event (the reject case).
    let b = host.remove("events", event(1, 1, 50));
    let outcome = engine.derive(&b, &mut host);
    assert!(outcome.failures.is_empty());
    assert_eq!(host.rows("totals"), vec![vec![i64v(1), i64v(100)]]);
    assert_eq!(host.rows("lines"), vec![vec![i64v(1), i64v(0), i64v(100)]]);
}

#[test]
fn key_death_clears_all_output() {
    let mut engine = engine_with_draft();
    let mut host = FakeHost::default();

    let b = host.insert("events", event(1, 0, 100));
    engine.derive(&b, &mut host);
    let b = host.remove("events", event(1, 0, 100));
    let outcome = engine.derive(&b, &mut host);

    assert!(outcome.failures.is_empty());
    assert!(host.rows("totals").is_empty());
    assert!(host.rows("lines").is_empty());
}

// ── Telescoping invariant ────────────────────────────────────────────────

/// Σ of all applied deltas must always equal the current materialized
/// output — the `last_render` bookkeeping is the running sum.
#[test]
fn telescoping_sum_of_deltas_equals_output() {
    let mut engine = engine_with_draft();
    let mut host = FakeHost::default();

    // Running multiset balance per (table, row).
    let mut balance: HashMap<(String, Vec<CellValue>), i64> = HashMap::new();
    let track = |z: &ZSet, balance: &mut HashMap<(String, Vec<CellValue>), i64>| {
        for e in &z.entries {
            *balance.entry((e.table.clone(), e.row.clone())).or_default() += e.weight as i64;
        }
    };

    let script: Vec<(bool, Vec<CellValue>)> = vec![
        (true, event(1, 0, 10)),
        (true, event(1, 1, 20)),
        (true, event(2, 0, 5)),
        (false, event(1, 0, 10)),
        (true, event(1, 2, 40)),
        (true, event(3, 0, 1)),
        (false, event(2, 0, 5)),
        (false, event(1, 1, 20)),
    ];
    for (is_insert, row) in script {
        let batch = if is_insert {
            host.insert("events", row)
        } else {
            host.remove("events", row)
        };
        let outcome = engine.derive(&batch, &mut host);
        assert!(outcome.failures.is_empty());
        track(&outcome.delta, &mut balance);

        // Materialize the balance and compare to host contents.
        for table in ["totals", "lines"] {
            let mut expected: Vec<Vec<CellValue>> = balance
                .iter()
                .filter(|((t, _), &w)| t == table && w != 0)
                .flat_map(|((_, r), &w)| {
                    assert!(w > 0, "negative balance for {r:?}");
                    std::iter::repeat_n(r.clone(), w as usize)
                })
                .collect();
            expected.sort();
            let mut actual = host.rows(table);
            actual.sort();
            assert_eq!(actual, expected, "table '{table}' diverged from Σ deltas");
        }
    }
}

// ── Registration validation ──────────────────────────────────────────────

struct AdHoc {
    spec: ProjectionSpec,
}

impl Projection for AdHoc {
    fn spec(&self) -> ProjectionSpec {
        self.spec.clone()
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

fn ad_hoc(id: &str, sources: &[(&str, usize)], reads: &[&str], outputs: &[&str]) -> Box<AdHoc> {
    Box::new(AdHoc {
        spec: ProjectionSpec {
            id: id.into(),
            sources: sources
                .iter()
                .map(|&(t, c)| PartitionedSource { table: t.into(), partition_column: c })
                .collect(),
            reads: reads.iter().map(|&r| r.into()).collect(),
            outputs: outputs.iter().map(|&o| o.into()).collect(),
        },
    })
}

#[test]
fn duplicate_output_ownership_rejected() {
    let mut engine = engine_with_draft();
    let err = engine
        .register(ad_hoc("other", &[("events", 0)], &[], &["totals"]))
        .unwrap_err();
    assert!(err.to_string().contains("already owned by projection 'draft'"));
}

#[test]
fn duplicate_id_rejected() {
    let mut engine = engine_with_draft();
    let err = engine
        .register(ad_hoc("draft", &[("x", 0)], &[], &["y"]))
        .unwrap_err();
    assert!(err.to_string().contains("already registered"));
}

#[test]
fn self_cycle_rejected() {
    let mut engine = ProjectionEngine::new();
    let err = engine
        .register(ad_hoc("selfy", &[("t", 0)], &[], &["t"]))
        .unwrap_err();
    assert!(err.to_string().contains("own output"));
}

#[test]
fn graph_cycle_rejected_and_state_unchanged() {
    let mut engine = ProjectionEngine::new();
    engine.register(ad_hoc("a", &[("s", 0)], &[], &["t"])).unwrap();
    // b: t → s would close the cycle a→b→a.
    let err = engine.register(ad_hoc("b", &[("t", 0)], &[], &["s"])).unwrap_err();
    assert!(err.to_string().contains("cycle"));
    // Registration must not have committed anything of b.
    assert_eq!(engine.projection_ids().collect::<Vec<_>>(), vec!["a"]);
    assert_eq!(engine.owned_tables().collect::<Vec<_>>(), vec!["t"]);
}

// ── Ownership guard ──────────────────────────────────────────────────────

#[test]
fn guard_external_rejects_owned_table_writes() {
    let engine = engine_with_draft();
    let mut batch = ZSet::new();
    batch.insert("totals".into(), vec![i64v(1), i64v(999)]);
    let violation = engine.guard_external(&batch).unwrap_err();
    assert_eq!(violation.table, "totals");
    assert_eq!(violation.owner, "draft");

    let mut ok = ZSet::new();
    ok.insert("events".into(), event(1, 0, 1));
    assert!(engine.guard_external(&ok).is_ok());
}

// ── DAG cascade ──────────────────────────────────────────────────────────

#[test]
fn cascade_updates_downstream_in_same_pass() {
    let mut engine = engine_with_draft();
    engine.register(Box::new(DoubledProjection)).unwrap();
    let mut host = FakeHost::default();

    let batch = host.insert("events", event(1, 0, 100));
    let outcome = engine.derive(&batch, &mut host);
    assert!(outcome.failures.is_empty());

    assert_eq!(host.rows("totals"), vec![vec![i64v(1), i64v(100)]]);
    assert_eq!(host.rows("doubled"), vec![vec![i64v(1), i64v(200)]]);
    // The combined delta carries both levels — ONE notify shows a
    // consistent picture.
    assert!(outcome.delta.entries.iter().any(|e| e.table == "totals"));
    assert!(outcome.delta.entries.iter().any(|e| e.table == "doubled"));

    // Update flows through both levels.
    let batch = host.insert("events", event(1, 1, 50));
    engine.derive(&batch, &mut host);
    assert_eq!(host.rows("doubled"), vec![vec![i64v(1), i64v(300)]]);

    // Key death cascades too.
    let b1 = host.remove("events", event(1, 0, 100));
    engine.derive(&b1, &mut host);
    let b2 = host.remove("events", event(1, 1, 50));
    engine.derive(&b2, &mut host);
    assert!(host.rows("totals").is_empty());
    assert!(host.rows("doubled").is_empty());
}

#[test]
fn registration_order_does_not_matter_for_cascade() {
    // Register the downstream projection FIRST — topo order must fix it.
    let mut engine = ProjectionEngine::new();
    engine.register(Box::new(DoubledProjection)).unwrap();
    engine.register(Box::new(DraftProjection)).unwrap();
    let mut host = FakeHost::default();

    let batch = host.insert("events", event(1, 0, 100));
    let outcome = engine.derive(&batch, &mut host);
    assert!(outcome.failures.is_empty());
    assert_eq!(host.rows("doubled"), vec![vec![i64v(1), i64v(200)]]);
}

// ── Read tables ──────────────────────────────────────────────────────────

/// events(doc_id, customer_id) + reads customers(id, name)
/// → labels(doc_id, name).
struct LabelProjection;

impl Projection for LabelProjection {
    fn spec(&self) -> ProjectionSpec {
        ProjectionSpec {
            id: "labels".into(),
            sources: vec![PartitionedSource { table: "events".into(), partition_column: 0 }],
            reads: vec!["customers".into()],
            outputs: vec!["labels".into()],
        }
    }

    fn project(
        &self,
        key: &CellValue,
        inputs: &Inputs,
        ctx: &ReadCtx<'_>,
        _cache: &mut FoldCache,
    ) -> Result<Vec<OutputRow>, String> {
        let customers = ctx.rows("customers")?;
        let mut out = Vec::new();
        for e in inputs.rows("events") {
            let name = customers
                .iter()
                .find(|c| c[0] == e[1])
                .map(|c| c[1].clone())
                .unwrap_or(CellValue::Null);
            out.push(("labels".to_string(), vec![key.clone(), name]));
        }
        Ok(out)
    }
}

#[test]
fn read_table_change_rerenders_live_keys() {
    let mut engine = ProjectionEngine::new();
    engine.register(Box::new(LabelProjection)).unwrap();
    let mut host = FakeHost::default();

    host.insert("customers", vec![i64v(7), CellValue::Str("Alice".into())]);
    let batch = host.insert("events", vec![i64v(1), i64v(7)]);
    engine.derive(&batch, &mut host);
    assert_eq!(host.rows("labels"), vec![vec![i64v(1), CellValue::Str("Alice".into())]]);

    // Rename the customer — only the read table changes.
    let b1 = host.remove("customers", vec![i64v(7), CellValue::Str("Alice".into())]);
    let mut batch = b1;
    batch.extend(host.insert("customers", vec![i64v(7), CellValue::Str("Alicia".into())]));
    let outcome = engine.derive(&batch, &mut host);
    assert!(outcome.failures.is_empty());
    assert_eq!(host.rows("labels"), vec![vec![i64v(1), CellValue::Str("Alicia".into())]]);
}

#[test]
fn undeclared_read_is_a_failure_and_output_untouched() {
    struct Sneaky;
    impl Projection for Sneaky {
        fn spec(&self) -> ProjectionSpec {
            ProjectionSpec {
                id: "sneaky".into(),
                sources: vec![PartitionedSource { table: "events".into(), partition_column: 0 }],
                reads: vec![],
                outputs: vec!["out".into()],
            }
        }
        fn project(
            &self,
            _key: &CellValue,
            _inputs: &Inputs,
            ctx: &ReadCtx<'_>,
            _cache: &mut FoldCache,
        ) -> Result<Vec<OutputRow>, String> {
            ctx.rows("customers")?; // not declared → must error
            Ok(vec![])
        }
    }

    let mut engine = ProjectionEngine::new();
    engine.register(Box::new(Sneaky)).unwrap();
    let mut host = FakeHost::default();

    let batch = host.insert("events", event(1, 0, 1));
    let outcome = engine.derive(&batch, &mut host);
    assert_eq!(outcome.failures.len(), 1);
    assert!(outcome.failures[0].message.contains("not declared"));
    assert!(host.rows("out").is_empty());
}

#[test]
fn undeclared_output_is_a_failure_and_output_untouched() {
    struct WrongTable;
    impl Projection for WrongTable {
        fn spec(&self) -> ProjectionSpec {
            ProjectionSpec {
                id: "wrong".into(),
                sources: vec![PartitionedSource { table: "events".into(), partition_column: 0 }],
                reads: vec![],
                outputs: vec!["out".into()],
            }
        }
        fn project(
            &self,
            key: &CellValue,
            _inputs: &Inputs,
            _ctx: &ReadCtx<'_>,
            _cache: &mut FoldCache,
        ) -> Result<Vec<OutputRow>, String> {
            Ok(vec![("elsewhere".to_string(), vec![key.clone()])])
        }
    }

    let mut engine = ProjectionEngine::new();
    engine.register(Box::new(WrongTable)).unwrap();
    let mut host = FakeHost::default();

    let batch = host.insert("events", event(1, 0, 1));
    let outcome = engine.derive(&batch, &mut host);
    assert_eq!(outcome.failures.len(), 1);
    assert!(outcome.failures[0].message.contains("undeclared output"));
    assert!(host.rows("elsewhere").is_empty());
}

#[test]
fn failure_keeps_previous_output() {
    struct FailOnThree;
    impl Projection for FailOnThree {
        fn spec(&self) -> ProjectionSpec {
            ProjectionSpec {
                id: "fragile".into(),
                sources: vec![PartitionedSource { table: "events".into(), partition_column: 0 }],
                reads: vec![],
                outputs: vec!["out".into()],
            }
        }
        fn project(
            &self,
            key: &CellValue,
            inputs: &Inputs,
            _ctx: &ReadCtx<'_>,
            _cache: &mut FoldCache,
        ) -> Result<Vec<OutputRow>, String> {
            let n = inputs.rows("events").len();
            if n >= 3 {
                return Err("cannot handle three events".into());
            }
            Ok(vec![("out".to_string(), vec![key.clone(), i64v(n as i64)])])
        }
    }

    let mut engine = ProjectionEngine::new();
    engine.register(Box::new(FailOnThree)).unwrap();
    let mut host = FakeHost::default();

    let b = host.insert("events", event(1, 0, 1));
    engine.derive(&b, &mut host);
    let b = host.insert("events", event(1, 1, 1));
    engine.derive(&b, &mut host);
    assert_eq!(host.rows("out"), vec![vec![i64v(1), i64v(2)]]);

    // Third event: project fails — previous output must survive.
    let b = host.insert("events", event(1, 2, 1));
    let outcome = engine.derive(&b, &mut host);
    assert_eq!(outcome.failures.len(), 1);
    assert_eq!(host.rows("out"), vec![vec![i64v(1), i64v(2)]]);
}

// ── Reset & rederive ─────────────────────────────────────────────────────

#[test]
fn reset_and_rederive_rebuilds_from_sources() {
    let mut engine = engine_with_draft();
    let mut host = FakeHost::default();

    let b = host.insert("events", event(1, 0, 100));
    engine.derive(&b, &mut host);

    // Simulate wholesale replacement: outputs now contain garbage and the
    // sources changed behind the engine's back.
    host.tables.insert("totals".into(), vec![vec![i64v(9), i64v(9)]]);
    host.tables.insert("lines".into(), vec![vec![i64v(9), i64v(9), i64v(9)]]);
    host.tables.insert(
        "events".into(),
        vec![event(2, 0, 11), event(2, 1, 22)],
    );

    let outcome = engine.reset_and_rederive(&mut host);
    assert!(outcome.failures.is_empty());
    assert_eq!(host.rows("totals"), vec![vec![i64v(2), i64v(33)]]);
    assert_eq!(host.rows("lines").len(), 2);
}

// ── Fold-cache lifecycle (§9.3 execution memo) ───────────────────────────

/// Emits `probe(key, had_memo)` and stamps the cache — makes memo
/// survival observable from outside. Deliberately violates the
/// "cache never changes the result" rule; that is the point of the
/// probe, and these tests assert the ENGINE's lifecycle, not a render.
struct CacheProbe;

impl Projection for CacheProbe {
    fn spec(&self) -> ProjectionSpec {
        ProjectionSpec {
            id: "cache_probe".into(),
            sources: vec![PartitionedSource { table: "events".into(), partition_column: 0 }],
            reads: vec![],
            outputs: vec!["probe".into()],
        }
    }

    fn project(
        &self,
        key: &CellValue,
        _inputs: &Inputs,
        _ctx: &ReadCtx<'_>,
        cache: &mut FoldCache,
    ) -> Result<Vec<OutputRow>, String> {
        let had = cache.get::<u8>().is_some();
        cache.put(1u8);
        Ok(vec![("probe".into(), vec![key.clone(), i64v(had as i64)])])
    }
}

#[test]
fn fold_cache_dies_with_the_partition() {
    let mut host = FakeHost::default();
    let mut engine = ProjectionEngine::new();
    engine.register(Box::new(CacheProbe)).unwrap();

    let first = vec![i64v(1), i64v(0), i64v(5)];
    let b = host.insert("events", first.clone());
    assert!(engine.derive(&b, &mut host).failures.is_empty());
    assert_eq!(host.rows("probe"), vec![vec![i64v(1), i64v(0)]], "first run: no memo");

    let second = vec![i64v(1), i64v(1), i64v(7)];
    let b = host.insert("events", second.clone());
    assert!(engine.derive(&b, &mut host).failures.is_empty());
    assert_eq!(host.rows("probe"), vec![vec![i64v(1), i64v(1)]], "memo survives per partition");

    // Last source row gone → partition dies → memo must die with it.
    let mut b = host.remove("events", first);
    b.extend(host.remove("events", second));
    assert!(engine.derive(&b, &mut host).failures.is_empty());
    assert!(host.rows("probe").is_empty(), "no rows → no output");

    let b = host.insert("events", vec![i64v(1), i64v(2), i64v(9)]);
    assert!(engine.derive(&b, &mut host).failures.is_empty());
    assert_eq!(
        host.rows("probe"),
        vec![vec![i64v(1), i64v(0)]],
        "reborn partition starts without a memo"
    );
}

#[test]
fn reset_and_rederive_clears_fold_caches() {
    let mut host = FakeHost::default();
    let mut engine = ProjectionEngine::new();
    engine.register(Box::new(CacheProbe)).unwrap();

    let b = host.insert("events", vec![i64v(1), i64v(0), i64v(5)]);
    assert!(engine.derive(&b, &mut host).failures.is_empty());
    let b = host.insert("events", vec![i64v(1), i64v(1), i64v(6)]);
    assert!(engine.derive(&b, &mut host).failures.is_empty());
    assert_eq!(host.rows("probe"), vec![vec![i64v(1), i64v(1)]], "memo present");

    // Wholesale replacement: seq lists could match the new reality by
    // coincidence — every memo must go.
    let outcome = engine.reset_and_rederive(&mut host);
    assert!(outcome.failures.is_empty());
    assert_eq!(
        host.rows("probe"),
        vec![vec![i64v(1), i64v(0)]],
        "replacement drops every memo"
    );
}
