//! Snapshot tests that verify optimizer behavior via span pretty-printing.
//!
//! Each test executes a query, pretty-prints the span tree, and compares
//! against an expected snapshot. This makes it immediately visible which
//! indexes are used, how many rows survive each stage, and whether filters
//! are pushed down.

use std::collections::HashMap;

use sql_engine::execute::{self, ExecutionContext};
use sql_engine::planner;
use sql_engine::planner::requirement::RequirementRegistry;
use sql_engine::storage::{CellValue, Table};
use sql_parser::parser;
use sql_engine::schema::{ColumnSchema, DataType, IndexSchema, IndexType, TableSchema};

// ── Test harness ──────────────────────────────────────────────────────────

fn make_table_schema(name: &str, cols: &[(&str, DataType, bool)]) -> TableSchema {
    TableSchema {
        name: name.into(),
        columns: cols.iter().map(|(n, dt, nullable)| ColumnSchema {
            name: (*n).into(), data_type: *dt, nullable: *nullable,
        }).collect(),
        primary_key: vec![0],
        indexes: vec![],
    }
}

struct TestDb {
    tables: HashMap<String, Table>,
    table_schemas: HashMap<String, TableSchema>,
    requirements: RequirementRegistry,
}

impl TestDb {
    fn new() -> Self { Self { tables: HashMap::new(), table_schemas: HashMap::new(), requirements: RequirementRegistry::new() } }

    fn add_table(&mut self, name: &str, cols: &[(&str, DataType, bool)]) -> &mut Table {
        let ts = make_table_schema(name, cols);
        self.table_schemas.insert(name.into(), ts.clone());
        self.tables.insert(name.into(), Table::new(ts));
        self.tables.get_mut(name).unwrap()
    }

    fn add_table_with_indexes(&mut self, name: &str, cols: &[(&str, DataType, bool)], indexes: Vec<IndexSchema>) -> &mut Table {
        let mut ts = make_table_schema(name, cols);
        ts.indexes = indexes;
        self.table_schemas.insert(name.into(), ts.clone());
        self.tables.insert(name.into(), Table::new(ts));
        self.tables.get_mut(name).unwrap()
    }

    fn trace(&self, sql: &str) -> String {
        let ast = parser::parse(sql).expect("parse failed");
        let plan = planner::sql::plan(&ast, &self.table_schemas, &self.requirements).expect("plan failed");
        let mut ctx = ExecutionContext::new(&self.tables);
        execute::execute_plan(&mut ctx, &plan).expect("execute failed");
        ctx.pretty_print()
    }
}

fn i(v: i64) -> CellValue { CellValue::I64(v) }
fn s(v: &str) -> CellValue { CellValue::Str(v.into()) }

/// Assert that the span trace matches the expected snapshot.
/// Trims trailing whitespace from both sides for clean comparison.
fn assert_trace(actual: &str, expected: &str) {
    let actual = actual.trim_end();
    let expected = expected.trim();
    assert_eq!(actual, expected, "\n\n--- ACTUAL ---\n{actual}\n\n--- EXPECTED ---\n{expected}\n");
}

// ── Test fixtures ─────────────────────────────────────────────────────────

fn make_db() -> TestDb {
    let mut db = TestDb::new();
    let users = db.add_table("users", &[
        ("id", DataType::I64, false),
        ("name", DataType::String, false),
        ("age", DataType::I64, true),
    ]);
    users.insert(&[i(1), s("Alice"), i(30)]).unwrap();
    users.insert(&[i(2), s("Bob"), i(25)]).unwrap();
    users.insert(&[i(3), s("Carol"), i(35)]).unwrap();
    users.insert(&[i(4), s("Dave"), CellValue::Null]).unwrap();

    let orders = db.add_table("orders", &[
        ("id", DataType::I64, false),
        ("user_id", DataType::I64, false),
        ("amount", DataType::I64, false),
    ]);
    orders.insert(&[i(10), i(1), i(100)]).unwrap();
    orders.insert(&[i(11), i(1), i(200)]).unwrap();
    orders.insert(&[i(12), i(2), i(50)]).unwrap();
    orders.insert(&[i(13), i(3), i(300)]).unwrap();
    db
}

fn make_indexed_db() -> TestDb {
    let mut db = TestDb::new();
    let users = db.add_table_with_indexes("users", &[
        ("id", DataType::I64, false),
        ("name", DataType::String, false),
        ("age", DataType::I64, true),
    ], vec![
        IndexSchema { name: Some("idx_id".into()), columns: vec![0], index_type: IndexType::BTree },
        IndexSchema { name: Some("idx_age".into()), columns: vec![2], index_type: IndexType::BTree },
    ]);
    users.insert(&[i(1), s("Alice"), i(30)]).unwrap();
    users.insert(&[i(2), s("Bob"), i(25)]).unwrap();
    users.insert(&[i(3), s("Carol"), i(35)]).unwrap();
    users.insert(&[i(4), s("Dave"), CellValue::Null]).unwrap();
    db
}

fn make_composite_indexed_db() -> TestDb {
    let mut db = TestDb::new();
    let events = db.add_table_with_indexes("events", &[
        ("user_id", DataType::I64, false),
        ("category", DataType::I64, false),
        ("score", DataType::I64, false),
    ], vec![
        IndexSchema { name: Some("idx_user_cat".into()), columns: vec![0, 1], index_type: IndexType::BTree },
    ]);
    events.insert(&[i(1), i(10), i(100)]).unwrap();
    events.insert(&[i(1), i(20), i(200)]).unwrap();
    events.insert(&[i(2), i(10), i(300)]).unwrap();
    events.insert(&[i(2), i(20), i(400)]).unwrap();
    events.insert(&[i(2), i(30), i(500)]).unwrap();
    db
}

fn make_hash_indexed_db() -> TestDb {
    let mut db = TestDb::new();
    let users = db.add_table_with_indexes("users", &[
        ("id", DataType::I64, false),
        ("name", DataType::String, false),
    ], vec![
        IndexSchema { name: Some("idx_id_hash".into()), columns: vec![0], index_type: IndexType::Hash },
        IndexSchema { name: Some("idx_id_btree".into()), columns: vec![0], index_type: IndexType::BTree },
    ]);
    users.insert(&[i(1), s("Alice")]).unwrap();
    users.insert(&[i(2), s("Bob")]).unwrap();
    users.insert(&[i(3), s("Carol")]).unwrap();
    db
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. INDEX SELECTION
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn index_btree_equality() {
    let db = make_indexed_db();
    assert_trace(&db.trace("SELECT users.name FROM users WHERE users.id = 2"), "
Execute
  Scan table=users method=BTree([0] prefix=1) rows=1
  Project columns=1 rows=1
");
}

#[test]
fn index_btree_range_gt() {
    let db = make_indexed_db();
    assert_trace(&db.trace("SELECT users.name FROM users WHERE users.age > 28"), "
Execute
  Scan table=users method=BTree([2] prefix=1) rows=2
  Project columns=1 rows=2
");
}

#[test]
fn index_btree_range_lt() {
    let db = make_indexed_db();
    assert_trace(&db.trace("SELECT users.name FROM users WHERE users.age < 30"), "
Execute
  Scan table=users method=BTree([2] prefix=1) rows=1
  Project columns=1 rows=1
");
}

#[test]
fn index_btree_range_gte() {
    let db = make_indexed_db();
    assert_trace(&db.trace("SELECT users.name FROM users WHERE users.age >= 30"), "
Execute
  Scan table=users method=BTree([2] prefix=1) rows=2
  Project columns=1 rows=2
");
}

#[test]
fn index_btree_range_lte() {
    let db = make_indexed_db();
    assert_trace(&db.trace("SELECT users.name FROM users WHERE users.age <= 30"), "
Execute
  Scan table=users method=BTree([2] prefix=1) rows=2
  Project columns=1 rows=2
");
}

#[test]
fn index_in_uses_btree() {
    let db = make_indexed_db();
    assert_trace(&db.trace("SELECT users.name FROM users WHERE users.id IN (1, 3)"), "
Execute
  Scan table=users method=BTree([0] prefix=1) rows=2
  Project columns=1 rows=2
");
}

#[test]
fn index_hash_preferred_over_btree_for_equality() {
    let db = make_hash_indexed_db();
    assert_trace(&db.trace("SELECT users.name FROM users WHERE users.id = 2"), "
Execute
  Scan table=users method=Hash([0] prefix=1) rows=1
  Project columns=1 rows=1
");
}

#[test]
fn no_index_falls_back_to_full_scan() {
    let db = make_indexed_db();
    // name column has no index
    assert_trace(&db.trace("SELECT users.id FROM users WHERE users.name = 'Bob'"), "
Execute
  Scan table=users method=Full rows=1
  Project columns=1 rows=1
");
}

#[test]
fn no_filter_full_scan() {
    let db = make_indexed_db();
    assert_trace(&db.trace("SELECT users.name FROM users"), "
Execute
  Scan table=users method=Full rows=4
  Project columns=1 rows=4
");
}

#[test]
fn index_no_match_zero_rows() {
    let db = make_indexed_db();
    // Missing key → index still used, 0 rows returned (not a full scan fallback)
    assert_trace(&db.trace("SELECT users.name FROM users WHERE users.id = 999"), "
Execute
  Scan table=users method=BTree([0] prefix=1) rows=0
  Project columns=1 rows=0
");
}

// ── Composite index ───────────────────────────────────────────────────────

#[test]
fn composite_index_full_key() {
    let db = make_composite_indexed_db();
    assert_trace(&db.trace(
        "SELECT events.score FROM events WHERE events.user_id = 2 AND events.category = 20",
    ), "
Execute
  Scan table=events method=BTree([0, 1] prefix=2) rows=1
  Project columns=1 rows=1
");
}

#[test]
fn composite_index_prefix_only() {
    let db = make_composite_indexed_db();
    // primary_key=[0] creates a Hash index on user_id which wins over BTree prefix
    assert_trace(&db.trace("SELECT events.score FROM events WHERE events.user_id = 1"), "
Execute
  Scan table=events method=Hash([0] prefix=1) rows=2
  Project columns=1 rows=2
");
}

#[test]
fn composite_index_prefix_eq_plus_range() {
    let db = make_composite_indexed_db();
    assert_trace(&db.trace(
        "SELECT events.score FROM events WHERE events.user_id = 2 AND events.category > 10",
    ), "
Execute
  Scan table=events method=BTree([0, 1] prefix=2) rows=2
  Project columns=1 rows=2
");
}

#[test]
fn composite_index_with_remaining_filter() {
    let db = make_composite_indexed_db();
    // Index covers (user_id, category), score filter applied as post-filter within scan
    assert_trace(&db.trace(
        "SELECT events.score FROM events WHERE events.user_id = 2 AND events.category >= 10 AND events.score > 350",
    ), "
Execute
  Scan table=events method=BTree([0, 1] prefix=2) rows=2
  Project columns=1 rows=2
");
}

#[test]
fn composite_index_gap_falls_back_to_full_scan() {
    let db = make_composite_indexed_db();
    // category only — can't use (user_id, category) without user_id prefix
    assert_trace(&db.trace("SELECT events.score FROM events WHERE events.category = 10"), "
Execute
  Scan table=events method=Full rows=2
  Project columns=1 rows=2
");
}

#[test]
fn composite_index_in_on_second_column() {
    let db = make_composite_indexed_db();
    assert_trace(&db.trace(
        "SELECT events.score FROM events WHERE events.user_id = 2 AND events.category IN (10, 30)",
    ), "
Execute
  Scan table=events method=BTree([0, 1] prefix=2) rows=2
  Project columns=1 rows=2
");
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. FILTER PUSHDOWN
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn single_table_filter_pushed_no_filter_span() {
    let db = make_db();
    // Single-table filter is pushed into scan — no separate Filter span
    assert_trace(&db.trace("SELECT users.name FROM users WHERE users.age > 28"), "
Execute
  Scan table=users method=Full rows=2
  Project columns=1 rows=2
");
}

#[test]
fn join_filter_pushed_to_right_table() {
    let db = make_db();
    // orders.amount > 100 pushed to orders scan
    assert_trace(&db.trace(
        "SELECT users.name, orders.amount \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         WHERE orders.amount > 100",
    ), "
Execute
  Scan table=users method=Full rows=4
  Scan table=orders method=Full rows=2
  Join rows_out=2
  Project columns=2 rows=2
");
}

#[test]
fn mixed_pushdown_both_tables_filtered() {
    let db = make_db();
    // users.age > 24 → pushed to users scan (3 rows: Alice, Bob, Carol)
    // orders.amount > 100 → pushed to orders scan (2 rows: 200, 300)
    assert_trace(&db.trace(
        "SELECT users.name, orders.amount \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         WHERE users.age > 24 AND orders.amount > 100",
    ), "
Execute
  Scan table=users method=Full rows=3
  Scan table=orders method=Full rows=2
  Join rows_out=2
  Project columns=2 rows=2
");
}

#[test]
fn pushdown_with_index() {
    let db = make_indexed_db();
    // Index on id → only 1 row scanned instead of 4
    assert_trace(&db.trace("SELECT users.name FROM users WHERE users.id = 2"), "
Execute
  Scan table=users method=BTree([0] prefix=1) rows=1
  Project columns=1 rows=1
");
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. ROW COUNTS THROUGH PIPELINE
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn row_counts_inner_join() {
    let db = make_db();
    // 4 users, 4 orders → join matches 4 (Alice×2, Bob×1, Carol×1)
    assert_trace(&db.trace(
        "SELECT users.name, orders.amount \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id",
    ), "
Execute
  Scan table=users method=Full rows=4
  Scan table=orders method=Full rows=4
  Join rows_out=4
  Project columns=2 rows=4
");
}

#[test]
fn row_counts_left_join() {
    let db = make_db();
    // 4 matched + 1 unmatched (Dave) = 5
    assert_trace(&db.trace(
        "SELECT users.name, orders.amount \
         FROM users \
         LEFT JOIN orders ON users.id = orders.user_id",
    ), "
Execute
  Scan table=users method=Full rows=4
  Scan table=orders method=Full rows=4
  Join rows_out=5
  Project columns=2 rows=5
");
}

#[test]
fn row_counts_join_then_filter() {
    let db = make_db();
    assert_trace(&db.trace(
        "SELECT users.name, orders.amount \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         WHERE orders.amount > 100",
    ), "
Execute
  Scan table=users method=Full rows=4
  Scan table=orders method=Full rows=2
  Join rows_out=2
  Project columns=2 rows=2
");
}

#[test]
fn row_counts_aggregate() {
    let db = make_db();
    assert_trace(&db.trace(
        "SELECT users.name, COUNT(orders.amount) \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         GROUP BY users.name",
    ), "
Execute
  Scan table=users method=Full rows=4
  Scan table=orders method=Full rows=4
  Join rows_out=4
  Aggregate groups=3
  Project columns=2 rows=3
");
}

#[test]
fn filter_reduces_rows() {
    let db = make_db();
    assert_trace(&db.trace("SELECT users.name FROM users WHERE users.age > 30"), "
Execute
  Scan table=users method=Full rows=1
  Project columns=1 rows=1
");
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. EXECUTION PLAN SHAPE
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn shape_simple_select() {
    let db = make_db();
    assert_trace(&db.trace("SELECT users.name FROM users"), "
Execute
  Scan table=users method=Full rows=4
  Project columns=1 rows=4
");
}

#[test]
fn shape_select_with_sort() {
    let db = make_db();
    assert_trace(&db.trace("SELECT users.name FROM users ORDER BY users.name"), "
Execute
  Scan table=users method=Full rows=4
  Sort rows=4
  Project columns=1 rows=4
");
}

#[test]
fn shape_aggregate() {
    let db = make_db();
    assert_trace(&db.trace("SELECT users.name, SUM(users.age) FROM users GROUP BY users.name"), "
Execute
  Scan table=users method=Full rows=4
  Aggregate groups=4
  Project columns=2 rows=4
");
}

#[test]
fn shape_join_with_sort() {
    let db = make_db();
    assert_trace(&db.trace(
        "SELECT users.name, orders.amount \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         ORDER BY orders.amount DESC",
    ), "
Execute
  Scan table=users method=Full rows=4
  Scan table=orders method=Full rows=4
  Join rows_out=4
  Sort rows=4
  Project columns=2 rows=4
");
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. NO UNNECESSARY OPERATIONS
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn no_filter_sort_aggregate_join_when_unnecessary() {
    let db = make_db();
    // Simple select: only Scan + Project, nothing else
    let trace = db.trace("SELECT users.name FROM users");
    assert!(!trace.contains("Filter"));
    assert!(!trace.contains("Sort"));
    assert!(!trace.contains("Aggregate"));
    assert!(!trace.contains("Join"));
    assert!(!trace.contains("Materialize"));
}

#[test]
fn no_filter_span_when_pushed_to_scan() {
    let db = make_db();
    let trace = db.trace("SELECT users.name FROM users WHERE users.age > 28");
    assert!(!trace.contains("Filter"), "filter should be pushed into scan, not separate");
}

#[test]
fn no_materialize_without_subquery() {
    let db = make_db();
    let trace = db.trace("SELECT users.name FROM users");
    assert!(!trace.contains("Materialize"));
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. SUBQUERY MATERIALIZATION
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn in_subquery_materialization() {
    let db = make_db();
    // Subquery: orders with amount > 100 → user_ids [1, 3]
    // Main: users with id IN (1, 3) → Hash index on PK
    assert_trace(&db.trace(
        "SELECT users.name FROM users \
         WHERE users.id IN (SELECT orders.user_id FROM orders WHERE orders.amount > 100)",
    ), "
Materialize step=0
  Execute
    Scan table=orders method=Full rows=2
    Project columns=1 rows=2
Execute
  Scan table=users method=Hash([0] prefix=1) rows=2
  Project columns=1 rows=2
");
}

#[test]
fn scalar_subquery_materialization() {
    let db = make_db();
    // Subquery: order 12 → Hash index on PK
    // Main: users with id = 2 → Hash index on PK
    assert_trace(&db.trace(
        "SELECT users.name FROM users \
         WHERE users.id = (SELECT orders.user_id FROM orders WHERE orders.id = 12)",
    ), "
Materialize step=0
  Execute
    Scan table=orders method=Hash([0] prefix=1) rows=1
    Project columns=1 rows=1
Execute
  Scan table=users method=Hash([0] prefix=1) rows=1
  Project columns=1 rows=1
");
}

// ── More index selection ──────────────────────────────────────────────────

#[test]
fn hash_not_used_for_range_falls_back_to_full_scan() {
    // Hash index can't do range queries
    let mut db = TestDb::new();
    let t = db.add_table_with_indexes("t", &[
        ("id", DataType::I64, false),
        ("val", DataType::I64, false),
    ], vec![
        IndexSchema { name: Some("idx_hash".into()), columns: vec![1], index_type: IndexType::Hash },
    ]);
    t.insert(&[i(1), i(10)]).unwrap();
    t.insert(&[i(2), i(20)]).unwrap();
    t.insert(&[i(3), i(30)]).unwrap();
    assert_trace(&db.trace("SELECT t.id FROM t WHERE t.val > 15"), "
Execute
  Scan table=t method=Full rows=2
  Project columns=1 rows=2
");
}

#[test]
fn not_equals_not_indexable_full_scan() {
    let db = make_indexed_db();
    // != is not indexable — falls back to full scan
    assert_trace(&db.trace("SELECT users.name FROM users WHERE users.id != 2"), "
Execute
  Scan table=users method=Full rows=3
  Project columns=1 rows=3
");
}

#[test]
fn or_on_single_table_rewritten_to_in() {
    let db = make_db();
    // OR on same column rewritten to IN → uses Hash index on PK
    assert_trace(&db.trace("SELECT users.name FROM users WHERE users.id = 1 OR users.id = 3"), "
Execute
  Scan table=users method=Hash([0] prefix=1) rows=2
  Project columns=1 rows=2
");
}

#[test]
fn and_with_two_indexed_columns_picks_best() {
    let db = make_indexed_db();
    // Both id and age are indexed; optimizer picks one
    // id = 2 → 1 row (better), age > 20 → 3 rows
    assert_trace(&db.trace("SELECT users.name FROM users WHERE users.id = 2 AND users.age > 20"), "
Execute
  Scan table=users method=BTree([0] prefix=1) rows=1
  Project columns=1 rows=1
");
}

// ── Filter pushdown edge cases ────────────────────────────────────────────

#[test]
fn or_across_tables_stays_as_post_filter() {
    let db = make_db();
    // OR across tables can't be pushed → post-filter on RowSet
    // Only Carol×300 passes: age=35>30 yes. Others: Alice(30)>30 no, Bob(25)>30 no, amounts<=200.
    assert_trace(&db.trace(
        "SELECT users.name, orders.amount \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         WHERE users.age > 30 OR orders.amount > 200",
    ), "
Execute
  Scan table=users method=Full rows=4
  Scan table=orders method=Full rows=4
  Join rows_out=4
  Filter rows_in=4 rows_out=1
  Project columns=2 rows=1
");
}

#[test]
fn not_equals_pushdown() {
    let db = make_db();
    assert_trace(&db.trace("SELECT users.name FROM users WHERE users.id != 1"), "
Execute
  Scan table=users method=Full rows=3
  Project columns=1 rows=3
");
}

// ── Row counts edge cases ─────────────────────────────────────────────────

#[test]
fn empty_table_scan() {
    let mut db = TestDb::new();
    db.add_table("empty", &[
        ("id", DataType::I64, false),
        ("val", DataType::String, false),
    ]);
    assert_trace(&db.trace("SELECT empty.id FROM empty"), "
Execute
  Scan table=empty method=Full rows=0
  Project columns=1 rows=0
");
}

#[test]
fn all_rows_filtered_out() {
    let db = make_db();
    assert_trace(&db.trace("SELECT users.name FROM users WHERE users.age > 999"), "
Execute
  Scan table=users method=Full rows=0
  Project columns=1 rows=0
");
}

#[test]
fn inner_join_no_matches() {
    let db = make_db();
    // Filter orders to empty → join produces 0
    assert_trace(&db.trace(
        "SELECT users.name, orders.amount \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         WHERE orders.amount > 9999",
    ), "
Execute
  Scan table=users method=Full rows=4
  Scan table=orders method=Full rows=0
  Join rows_out=0
  Project columns=2 rows=0
");
}

#[test]
fn left_join_all_unmatched() {
    let db = make_db();
    // Filter orders to empty → all users get NULL fill
    assert_trace(&db.trace(
        "SELECT users.name, orders.amount \
         FROM users \
         LEFT JOIN orders ON users.id = orders.user_id \
         WHERE orders.amount > 9999",
    ), "
Execute
  Scan table=users method=Full rows=4
  Scan table=orders method=Full rows=0
  Join rows_out=4
  Project columns=2 rows=4
");
}

// ── Limit ─────────────────────────────────────────────────────────────────

#[test]
fn limit_basic() {
    let db = make_db();
    assert_trace(&db.trace("SELECT users.name FROM users LIMIT 2"), "
Execute
  Scan table=users method=Full rows=4
  Project columns=1 rows=2
");
}

#[test]
fn limit_with_sort() {
    let db = make_db();
    assert_trace(&db.trace("SELECT users.name FROM users ORDER BY users.name LIMIT 2"), "
Execute
  Scan table=users method=Full rows=4
  Sort rows=4
  Project columns=1 rows=2
");
}

#[test]
fn limit_with_filter() {
    let db = make_db();
    assert_trace(&db.trace("SELECT users.name FROM users WHERE users.age > 24 ORDER BY users.name LIMIT 2"), "
Execute
  Scan table=users method=Full rows=3
  Sort rows=3
  Project columns=1 rows=2
");
}

#[test]
fn limit_zero() {
    let db = make_db();
    assert_trace(&db.trace("SELECT users.name FROM users LIMIT 0"), "
Execute
  Scan table=users method=Full rows=4
  Project columns=1 rows=0
");
}

#[test]
fn limit_larger_than_result() {
    let db = make_db();
    assert_trace(&db.trace("SELECT users.name FROM users LIMIT 100"), "
Execute
  Scan table=users method=Full rows=4
  Project columns=1 rows=4
");
}

#[test]
fn limit_with_join() {
    let db = make_db();
    assert_trace(&db.trace(
        "SELECT users.name, orders.amount \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         ORDER BY orders.amount DESC LIMIT 2",
    ), "
Execute
  Scan table=users method=Full rows=4
  Scan table=orders method=Full rows=4
  Join rows_out=4
  Sort rows=4
  Project columns=2 rows=2
");
}

#[test]
fn limit_with_aggregate() {
    let db = make_db();
    // Aggregate path: Aggregate → Project → Limit
    assert_trace(&db.trace(
        "SELECT users.name, SUM(users.age) FROM users GROUP BY users.name LIMIT 2",
    ), "
Execute
  Scan table=users method=Full rows=4
  Aggregate groups=4
  Project columns=2 rows=4
");
}

// ── Full pipeline combinations ────────────────────────────────────────────

#[test]
fn shape_join_filter_sort_limit() {
    let db = make_db();
    assert_trace(&db.trace(
        "SELECT users.name, orders.amount \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         WHERE orders.amount > 50 \
         ORDER BY orders.amount DESC \
         LIMIT 2",
    ), "
Execute
  Scan table=users method=Full rows=4
  Scan table=orders method=Full rows=3
  Join rows_out=3
  Sort rows=3
  Project columns=2 rows=2
");
}

#[test]
fn shape_aggregate_with_sort() {
    let db = make_db();
    // Aggregate path: Aggregate → Project → Sort (sort on projected columns)
    assert_trace(&db.trace(
        "SELECT users.name, SUM(orders.amount) \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         GROUP BY users.name \
         ORDER BY users.name",
    ), "
Execute
  Scan table=users method=Full rows=4
  Scan table=orders method=Full rows=4
  Join rows_out=4
  Aggregate groups=3
  Project columns=2 rows=3
  Sort rows=3
");
}

#[test]
fn index_with_join() {
    let mut db = TestDb::new();
    let users = db.add_table_with_indexes("users", &[
        ("id", DataType::I64, false),
        ("name", DataType::String, false),
    ], vec![
        IndexSchema { name: Some("idx_id".into()), columns: vec![0], index_type: IndexType::BTree },
    ]);
    users.insert(&[i(1), s("Alice")]).unwrap();
    users.insert(&[i(2), s("Bob")]).unwrap();
    users.insert(&[i(3), s("Carol")]).unwrap();
    let orders = db.add_table("orders", &[
        ("id", DataType::I64, false),
        ("user_id", DataType::I64, false),
        ("amount", DataType::I64, false),
    ]);
    orders.insert(&[i(10), i(1), i(100)]).unwrap();
    orders.insert(&[i(11), i(2), i(200)]).unwrap();

    // users.id > 1 uses BTree index, orders full scan
    assert_trace(&db.trace(
        "SELECT users.name, orders.amount \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         WHERE users.id > 1",
    ), "
Execute
  Scan table=users method=BTree([0] prefix=1) rows=2
  Scan table=orders method=Full rows=2
  Join rows_out=1
  Project columns=2 rows=1
");
}

// ── Subquery edge cases ───────────────────────────────────────────────────

#[test]
fn in_subquery_empty_result() {
    let db = make_db();
    // Empty subquery result → IN with empty list → Hash index on PK returns 0 rows
    assert_trace(&db.trace(
        "SELECT users.name FROM users \
         WHERE users.id IN (SELECT orders.user_id FROM orders WHERE orders.amount > 9999)",
    ), "
Materialize step=0
  Execute
    Scan table=orders method=Full rows=0
    Project columns=1 rows=0
Execute
  Scan table=users method=Hash([0] prefix=1) rows=0
  Project columns=1 rows=0
");
}

#[test]
fn scalar_subquery_gt() {
    let db = make_db();
    // Subquery: SELECT orders.amount FROM orders WHERE orders.id = 12 → amount=50
    // Main: users WHERE age > 50 → none
    assert_trace(&db.trace(
        "SELECT users.name FROM users \
         WHERE users.age > (SELECT orders.amount FROM orders WHERE orders.id = 12)",
    ), "
Materialize step=0
  Execute
    Scan table=orders method=Hash([0] prefix=1) rows=1
    Project columns=1 rows=1
Execute
  Scan table=users method=Full rows=0
  Project columns=1 rows=0
");
}

#[test]
fn in_subquery_with_index_on_main() {
    let mut db2 = TestDb::new();
    let users = db2.add_table_with_indexes("users", &[
        ("id", DataType::I64, false),
        ("name", DataType::String, false),
        ("age", DataType::I64, true),
    ], vec![
        IndexSchema { name: Some("idx_id".into()), columns: vec![0], index_type: IndexType::BTree },
    ]);
    users.insert(&[i(1), s("Alice"), i(30)]).unwrap();
    users.insert(&[i(2), s("Bob"), i(25)]).unwrap();
    users.insert(&[i(3), s("Carol"), i(35)]).unwrap();
    users.insert(&[i(4), s("Dave"), CellValue::Null]).unwrap();

    let orders = db2.add_table("orders", &[
        ("id", DataType::I64, false),
        ("user_id", DataType::I64, false),
        ("amount", DataType::I64, false),
    ]);
    orders.insert(&[i(10), i(1), i(100)]).unwrap();
    orders.insert(&[i(11), i(1), i(200)]).unwrap();
    orders.insert(&[i(12), i(2), i(50)]).unwrap();

    // IN subquery resolves to literal IN list → uses BTree index on users.id
    assert_trace(&db2.trace(
        "SELECT users.name FROM users \
         WHERE users.id IN (SELECT orders.user_id FROM orders WHERE orders.amount > 100)",
    ), "
Materialize step=0
  Execute
    Scan table=orders method=Full rows=1
    Project columns=1 rows=1
Execute
  Scan table=users method=BTree([0] prefix=1) rows=1
  Project columns=1 rows=1
");
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. TIMING SANITY
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn parent_duration_gte_children_sum() {
    let db = make_db();
    let ast = parser::parse(
        "SELECT users.name, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id"
    ).unwrap();
    let plan = planner::sql::plan(&ast, &db.table_schemas, &db.requirements).unwrap();
    let mut ctx = ExecutionContext::new(&db.tables);
    execute::execute_plan(&mut ctx, &plan).unwrap();

    fn check(span: &execute::Span) {
        let children_sum: u128 = span.children.iter().map(|c| c.duration.as_nanos()).sum();
        assert!(
            span.duration.as_nanos() >= children_sum,
            "parent {:?} duration {}ns < children sum {}ns",
            span.operation, span.duration.as_nanos(), children_sum,
        );
        for child in &span.children { check(child); }
    }
    for span in &ctx.spans { check(span); }
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. INDEX NESTED LOOP JOIN
// ═══════════════════════════════════════════════════════════════════════════

fn make_indexed_join_db() -> TestDb {
    let mut db = TestDb::new();
    let users = db.add_table("users", &[
        ("id", DataType::I64, false),
        ("name", DataType::String, false),
    ]);
    users.insert(&[i(1), s("Alice")]).unwrap();
    users.insert(&[i(2), s("Bob")]).unwrap();
    users.insert(&[i(3), s("Carol")]).unwrap();

    let orders = db.add_table_with_indexes("orders", &[
        ("id", DataType::I64, false),
        ("user_id", DataType::I64, false),
        ("amount", DataType::I64, false),
    ], vec![
        IndexSchema { name: Some("idx_user_id".into()), columns: vec![1], index_type: IndexType::Hash },
    ]);
    orders.insert(&[i(10), i(1), i(100)]).unwrap();
    orders.insert(&[i(11), i(1), i(200)]).unwrap();
    orders.insert(&[i(12), i(2), i(50)]).unwrap();
    orders.insert(&[i(13), i(3), i(300)]).unwrap();

    db
}

#[test]
fn index_nested_loop_join_inner() {
    let db = make_indexed_join_db();
    // Orders has Hash index on user_id → no Scan of orders, index lookup per user row
    assert_trace(&db.trace(
        "SELECT users.name, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id",
    ), "
Execute
  Scan table=users method=Full rows=3
  Join rows_out=4
  Project columns=2 rows=4
");
}

#[test]
fn index_nested_loop_join_left() {
    let mut db = make_indexed_join_db();
    // Add a user with no orders
    db.tables.get_mut("users").unwrap()
        .insert(&[i(4), s("Dave")]).unwrap();

    assert_trace(&db.trace(
        "SELECT users.name, orders.amount FROM users LEFT JOIN orders ON users.id = orders.user_id",
    ), "
Execute
  Scan table=users method=Full rows=4
  Join rows_out=5
  Project columns=2 rows=5
");
}

#[test]
fn index_nested_loop_join_with_pre_filter() {
    let db = make_indexed_join_db();
    // users.id > 1 filters users first via PK index, then index-NLJ on orders
    assert_trace(&db.trace(
        "SELECT users.name, orders.amount FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         WHERE users.id > 1",
    ), "
Execute
  Scan table=users method=Full rows=2
  Join rows_out=2
  Project columns=2 rows=2
");
}

#[test]
fn index_nested_loop_join_no_matches() {
    let db = make_indexed_join_db();
    // user_id 99 has no orders → inner join yields 0 rows
    assert_trace(&db.trace(
        "SELECT users.name, orders.amount FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         WHERE users.id = 99",
    ), "
Execute
  Scan table=users method=Hash([0] prefix=1) rows=0
  Join rows_out=0
  Project columns=2 rows=0
");
}

#[test]
fn nested_loop_fallback_no_index_on_join_col() {
    // No index on user_id → regular nested loop join with full scan
    let mut db = TestDb::new();
    let users = db.add_table("users", &[
        ("id", DataType::I64, false),
        ("name", DataType::String, false),
    ]);
    users.insert(&[i(1), s("Alice")]).unwrap();
    users.insert(&[i(2), s("Bob")]).unwrap();

    let orders = db.add_table("orders", &[
        ("id", DataType::I64, false),
        ("user_id", DataType::I64, false),
        ("amount", DataType::I64, false),
    ]);
    orders.insert(&[i(10), i(1), i(100)]).unwrap();
    orders.insert(&[i(11), i(2), i(200)]).unwrap();

    // orders has no index on user_id → both sides are scanned
    assert_trace(&db.trace(
        "SELECT users.name, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id",
    ), "
Execute
  Scan table=users method=Full rows=2
  Scan table=orders method=Full rows=2
  Join rows_out=2
  Project columns=2 rows=2
");
}
