//! Snapshot tests for the query planner's pretty-printed execution plans.
//!
//! Each test parses SQL, runs the planner (including all optimization passes),
//! and compares the pretty-printed plan against an expected snapshot.
//! This makes it immediately visible which scan method, join strategy,
//! filter placement, and lookup method the planner chose.

use std::collections::HashMap;

use sql_engine::planner;
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
    table_schemas: HashMap<String, TableSchema>,
}

impl TestDb {
    fn new() -> Self { Self { table_schemas: HashMap::new() } }

    fn add_table(&mut self, name: &str, cols: &[(&str, DataType, bool)]) {
        let ts = make_table_schema(name, cols);
        self.table_schemas.insert(name.into(), ts);
    }

    fn add_table_with_indexes(&mut self, name: &str, cols: &[(&str, DataType, bool)], indexes: Vec<IndexSchema>) {
        let mut ts = make_table_schema(name, cols);
        ts.indexes = indexes;
        self.table_schemas.insert(name.into(), ts);
    }

    fn plan(&self, sql: &str) -> String {
        let ast = parser::parse(sql).expect("parse failed");
        let plan = planner::plan(&ast, &self.table_schemas).expect("plan failed");
        plan.pretty_print()
    }
}

fn assert_plan(actual: &str, expected: &str) {
    let actual = actual.trim_end();
    let expected = expected.trim();
    assert_eq!(actual, expected, "\n\n--- ACTUAL ---\n{actual}\n\n--- EXPECTED ---\n{expected}\n");
}

// ── Test fixtures ─────────────────────────────────────────────────────────

fn make_db() -> TestDb {
    let mut db = TestDb::new();
    db.add_table("users", &[
        ("id", DataType::I64, false),
        ("name", DataType::String, false),
        ("age", DataType::I64, true),
    ]);
    db.add_table("orders", &[
        ("id", DataType::I64, false),
        ("user_id", DataType::I64, false),
        ("amount", DataType::I64, false),
    ]);
    db
}

fn make_indexed_db() -> TestDb {
    let mut db = TestDb::new();
    db.add_table_with_indexes("users", &[
        ("id", DataType::I64, false),
        ("name", DataType::String, false),
        ("age", DataType::I64, true),
    ], vec![
        IndexSchema { name: Some("idx_id".into()), columns: vec![0], index_type: IndexType::BTree },
        IndexSchema { name: Some("idx_age".into()), columns: vec![2], index_type: IndexType::BTree },
    ]);
    db
}

fn make_composite_indexed_db() -> TestDb {
    let mut db = TestDb::new();
    db.add_table_with_indexes("events", &[
        ("user_id", DataType::I64, false),
        ("category", DataType::I64, false),
        ("score", DataType::I64, false),
    ], vec![
        IndexSchema { name: Some("idx_user_cat".into()), columns: vec![0, 1], index_type: IndexType::BTree },
    ]);
    db
}

fn make_hash_indexed_db() -> TestDb {
    let mut db = TestDb::new();
    db.add_table_with_indexes("users", &[
        ("id", DataType::I64, false),
        ("name", DataType::String, false),
    ], vec![
        IndexSchema { name: Some("idx_id_hash".into()), columns: vec![0], index_type: IndexType::Hash },
        IndexSchema { name: Some("idx_id_btree".into()), columns: vec![0], index_type: IndexType::BTree },
    ]);
    db
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. SIMPLE SCANS
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn simple_select_all_columns() {
    let db = make_db();
    assert_plan(&db.plan("SELECT users.id, users.name, users.age FROM users"), "
Select
  Scan table=users scan=Full
  Output [users.id, users.name, users.age]
");
}

#[test]
fn simple_select_single_column() {
    let db = make_db();
    assert_plan(&db.plan("SELECT users.name FROM users"), "
Select
  Scan table=users scan=Full
  Output [users.name]
");
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. FILTER PUSHDOWN
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn single_table_filter_pushed_to_pre_filter() {
    let db = make_db();
    assert_plan(&db.plan("SELECT users.name FROM users WHERE users.age > 28"), "
Select
  Scan table=users scan=Full
    pre_filter: users.age > 28
  Output [users.name]
");
}

#[test]
fn and_filter_pushed_to_single_table() {
    let db = make_db();
    assert_plan(&db.plan(
        "SELECT users.name FROM users WHERE users.age > 18 AND users.name = 'Alice'",
    ), "
Select
  Scan table=users scan=Full
    pre_filter: (users.age > 18 AND users.name = 'Alice')
  Output [users.name]
");
}

#[test]
fn cross_table_filter_stays_as_post_filter() {
    let db = make_db();
    assert_plan(&db.plan(
        "SELECT users.name, orders.amount \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         WHERE users.age > 30 OR orders.amount > 200",
    ), "
Select
  Scan table=users scan=Full
  Join type=Inner strategy=NestedLoop table=orders scan=Full
    on: users.id = orders.user_id
  Filter: (users.age > 30 OR orders.amount > 200)
  Output [users.name, orders.amount]
");
}

#[test]
fn mixed_pushdown_each_table_gets_its_filter() {
    let db = make_db();
    assert_plan(&db.plan(
        "SELECT users.name, orders.amount \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         WHERE users.age > 24 AND orders.amount > 100",
    ), "
Select
  Scan table=users scan=Full
    pre_filter: users.age > 24
  Join type=Inner strategy=NestedLoop table=orders scan=Full
    pre_filter: orders.amount > 100
    on: users.id = orders.user_id
  Output [users.name, orders.amount]
");
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. INDEX SELECTION — BTree
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn index_btree_equality() {
    let db = make_indexed_db();
    assert_plan(&db.plan("SELECT users.name FROM users WHERE users.id = 2"), "
Select
  Scan table=users scan=BTree([0] prefix=1 lookup=FullKeyEq)
    index_preds: [users.id = 2]
  Output [users.name]
");
}

#[test]
fn index_btree_range_gt() {
    let db = make_indexed_db();
    assert_plan(&db.plan("SELECT users.name FROM users WHERE users.age > 28"), "
Select
  Scan table=users scan=BTree([2] prefix=1 lookup=PrefixRange)
    index_preds: [users.age > 28]
  Output [users.name]
");
}

#[test]
fn index_btree_range_lt() {
    let db = make_indexed_db();
    assert_plan(&db.plan("SELECT users.name FROM users WHERE users.age < 30"), "
Select
  Scan table=users scan=BTree([2] prefix=1 lookup=PrefixRange)
    index_preds: [users.age < 30]
  Output [users.name]
");
}

#[test]
fn index_btree_range_gte() {
    let db = make_indexed_db();
    assert_plan(&db.plan("SELECT users.name FROM users WHERE users.age >= 30"), "
Select
  Scan table=users scan=BTree([2] prefix=1 lookup=PrefixRange)
    index_preds: [users.age >= 30]
  Output [users.name]
");
}

#[test]
fn index_btree_range_lte() {
    let db = make_indexed_db();
    assert_plan(&db.plan("SELECT users.name FROM users WHERE users.age <= 30"), "
Select
  Scan table=users scan=BTree([2] prefix=1 lookup=PrefixRange)
    index_preds: [users.age <= 30]
  Output [users.name]
");
}

#[test]
fn index_in_uses_btree() {
    let db = make_indexed_db();
    assert_plan(&db.plan("SELECT users.name FROM users WHERE users.id IN (1, 3)"), "
Select
  Scan table=users scan=BTree([0] prefix=1 lookup=InMultiLookup)
    index_preds: [users.id IN (1, 3)]
  Output [users.name]
");
}

#[test]
fn no_index_for_unindexed_column() {
    let db = make_indexed_db();
    assert_plan(&db.plan("SELECT users.id FROM users WHERE users.name = 'Bob'"), "
Select
  Scan table=users scan=Full
    pre_filter: users.name = 'Bob'
  Output [users.id]
");
}

#[test]
fn no_filter_full_scan() {
    let db = make_indexed_db();
    assert_plan(&db.plan("SELECT users.name FROM users"), "
Select
  Scan table=users scan=Full
  Output [users.name]
");
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. INDEX SELECTION — Hash
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn hash_preferred_over_btree_for_equality() {
    let db = make_hash_indexed_db();
    assert_plan(&db.plan("SELECT users.name FROM users WHERE users.id = 2"), "
Select
  Scan table=users scan=Hash([0] prefix=1 lookup=FullKeyEq)
    index_preds: [users.id = 2]
  Output [users.name]
");
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. COMPOSITE INDEX
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn composite_index_full_key_eq() {
    let db = make_composite_indexed_db();
    assert_plan(&db.plan(
        "SELECT events.score FROM events WHERE events.user_id = 2 AND events.category = 20",
    ), "
Select
  Scan table=events scan=BTree([0, 1] prefix=2 lookup=FullKeyEq)
    index_preds: [events.user_id = 2, events.category = 20]
  Output [events.score]
");
}

#[test]
fn composite_index_prefix_eq() {
    let db = make_composite_indexed_db();
    // user_id only → PK Hash index wins over BTree prefix
    assert_plan(&db.plan("SELECT events.score FROM events WHERE events.user_id = 1"), "
Select
  Scan table=events scan=Hash([0] prefix=1 lookup=FullKeyEq)
    index_preds: [events.user_id = 1]
  Output [events.score]
");
}

#[test]
fn composite_index_prefix_eq_plus_range() {
    let db = make_composite_indexed_db();
    assert_plan(&db.plan(
        "SELECT events.score FROM events WHERE events.user_id = 2 AND events.category > 10",
    ), "
Select
  Scan table=events scan=BTree([0, 1] prefix=2 lookup=PrefixRange)
    index_preds: [events.user_id = 2, events.category > 10]
  Output [events.score]
");
}

#[test]
fn composite_index_with_remaining_filter() {
    let db = make_composite_indexed_db();
    assert_plan(&db.plan(
        "SELECT events.score FROM events \
         WHERE events.user_id = 2 AND events.category >= 10 AND events.score > 350",
    ), "
Select
  Scan table=events scan=BTree([0, 1] prefix=2 lookup=PrefixRange)
    pre_filter: events.score > 350
    index_preds: [events.user_id = 2, events.category >= 10]
  Output [events.score]
");
}

#[test]
fn composite_index_gap_falls_back_to_full_scan() {
    let db = make_composite_indexed_db();
    assert_plan(&db.plan("SELECT events.score FROM events WHERE events.category = 10"), "
Select
  Scan table=events scan=Full
    pre_filter: events.category = 10
  Output [events.score]
");
}

#[test]
fn composite_index_in_on_second_column() {
    let db = make_composite_indexed_db();
    assert_plan(&db.plan(
        "SELECT events.score FROM events WHERE events.user_id = 2 AND events.category IN (10, 30)",
    ), "
Select
  Scan table=events scan=BTree([0, 1] prefix=2 lookup=InMultiLookup)
    index_preds: [events.user_id = 2, events.category IN (10, 30)]
  Output [events.score]
");
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. JOIN STRATEGIES
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn nested_loop_join_no_index() {
    let db = make_db();
    assert_plan(&db.plan(
        "SELECT users.name, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id",
    ), "
Select
  Scan table=users scan=Full
  Join type=Inner strategy=NestedLoop table=orders scan=Full
    on: users.id = orders.user_id
  Output [users.name, orders.amount]
");
}

#[test]
fn index_nested_loop_join() {
    let mut db = make_db();
    db.add_table_with_indexes("orders", &[
        ("id", DataType::I64, false),
        ("user_id", DataType::I64, false),
        ("amount", DataType::I64, false),
    ], vec![
        IndexSchema { name: Some("idx_user_id".into()), columns: vec![1], index_type: IndexType::Hash },
    ]);
    assert_plan(&db.plan(
        "SELECT users.name, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id",
    ), "
Select
  Scan table=users scan=Full
  Join type=Inner strategy=IndexLookup(Hash[1]) table=orders scan=Full
    on: users.id = orders.user_id
  Output [users.name, orders.amount]
");
}

#[test]
fn left_join() {
    let db = make_db();
    assert_plan(&db.plan(
        "SELECT users.name, orders.amount FROM users LEFT JOIN orders ON users.id = orders.user_id",
    ), "
Select
  Scan table=users scan=Full
  Join type=Left strategy=NestedLoop table=orders scan=Full
    on: users.id = orders.user_id
  Output [users.name, orders.amount]
");
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. AGGREGATES
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn group_by_with_aggregate() {
    let db = make_db();
    assert_plan(&db.plan(
        "SELECT users.name, SUM(users.age) FROM users GROUP BY users.name",
    ), "
Select
  Scan table=users scan=Full
  GroupBy [users.name]
  Aggregate SUM(users.age)
  Output [users.name, SUM(users.age)]
");
}

#[test]
fn count_with_group_by() {
    let db = make_db();
    assert_plan(&db.plan(
        "SELECT users.name, COUNT(users.id) FROM users GROUP BY users.name",
    ), "
Select
  Scan table=users scan=Full
  GroupBy [users.name]
  Aggregate COUNT(users.id)
  Output [users.name, COUNT(users.id)]
");
}

#[test]
fn min_max_aggregates() {
    let db = make_db();
    assert_plan(&db.plan(
        "SELECT users.name, MIN(users.age), MAX(users.age) FROM users GROUP BY users.name",
    ), "
Select
  Scan table=users scan=Full
  GroupBy [users.name]
  Aggregate MIN(users.age)
  Aggregate MAX(users.age)
  Output [users.name, MIN(users.age), MAX(users.age)]
");
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. ORDER BY
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn order_by_asc() {
    let db = make_db();
    assert_plan(&db.plan("SELECT users.name FROM users ORDER BY users.name"), "
Select
  Scan table=users scan=Full
  OrderBy [users.name ASC]
  Output [users.name]
");
}

#[test]
fn order_by_desc() {
    let db = make_db();
    assert_plan(&db.plan("SELECT users.name FROM users ORDER BY users.name DESC"), "
Select
  Scan table=users scan=Full
  OrderBy [users.name DESC]
  Output [users.name]
");
}

// ═══════════════════════════════════════════════════════════════════════════
// 9. LIMIT
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn limit_basic() {
    let db = make_db();
    assert_plan(&db.plan("SELECT users.name FROM users LIMIT 2"), "
Select
  Scan table=users scan=Full
  Limit 2
  Output [users.name]
");
}

#[test]
fn limit_with_sort() {
    let db = make_db();
    assert_plan(&db.plan("SELECT users.name FROM users ORDER BY users.name LIMIT 2"), "
Select
  Scan table=users scan=Full
  OrderBy [users.name ASC]
  Limit 2
  Output [users.name]
");
}

// ═══════════════════════════════════════════════════════════════════════════
// 10. OR → IN REWRITE
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn or_rewritten_to_in() {
    let db = make_db();
    assert_plan(&db.plan(
        "SELECT users.name FROM users WHERE users.id = 1 OR users.id = 3",
    ), "
Select
  Scan table=users scan=Hash([0] prefix=1 lookup=InMultiLookup)
    index_preds: [users.id IN (1, 3)]
  Output [users.name]
");
}

// ═══════════════════════════════════════════════════════════════════════════
// 11. SUBQUERY MATERIALIZATION
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn in_subquery_materialization() {
    let db = make_db();
    // InMaterialized is a placeholder — resolved at execution time, not at plan time.
    // So it stays as a pre_filter, not an index lookup.
    assert_plan(&db.plan(
        "SELECT users.name FROM users \
         WHERE users.id IN (SELECT orders.user_id FROM orders WHERE orders.amount > 100)",
    ), "
Materialize step=0 kind=List
  Scan table=orders scan=Full
    pre_filter: orders.amount > 100
  Output [orders.user_id]
Select
  Scan table=users scan=Full
    pre_filter: users.id IN $mat0
  Output [users.name]
");
}

#[test]
fn scalar_subquery_materialization() {
    let db = make_db();
    // CompareMaterialized is a placeholder — resolved at execution time, not at plan time.
    // So it stays as a pre_filter, not an index lookup.
    assert_plan(&db.plan(
        "SELECT users.name FROM users \
         WHERE users.id = (SELECT orders.user_id FROM orders WHERE orders.id = 12)",
    ), "
Materialize step=0 kind=Scalar
  Scan table=orders scan=Hash([0] prefix=1 lookup=FullKeyEq)
    index_preds: [orders.id = 12]
  Output [orders.user_id]
Select
  Scan table=users scan=Full
    pre_filter: users.id = $mat0
  Output [users.name]
");
}

// ═══════════════════════════════════════════════════════════════════════════
// 12. FULL PIPELINE COMBINATIONS
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn join_filter_sort_limit() {
    let db = make_db();
    assert_plan(&db.plan(
        "SELECT users.name, orders.amount \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         WHERE orders.amount > 50 \
         ORDER BY orders.amount DESC \
         LIMIT 2",
    ), "
Select
  Scan table=users scan=Full
  Join type=Inner strategy=NestedLoop table=orders scan=Full
    pre_filter: orders.amount > 50
    on: users.id = orders.user_id
  OrderBy [orders.amount DESC]
  Limit 2
  Output [users.name, orders.amount]
");
}

#[test]
fn aggregate_with_sort() {
    let db = make_db();
    assert_plan(&db.plan(
        "SELECT users.name, SUM(orders.amount) \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         GROUP BY users.name \
         ORDER BY users.name",
    ), "
Select
  Scan table=users scan=Full
  Join type=Inner strategy=NestedLoop table=orders scan=Full
    on: users.id = orders.user_id
  GroupBy [users.name]
  Aggregate SUM(orders.amount)
  OrderBy [users.name ASC]
  Output [users.name, SUM(orders.amount)]
");
}

#[test]
fn index_scan_with_join() {
    let db = make_indexed_db();
    db.table_schemas.get("users").unwrap(); // just assert fixture is present
    let mut db2 = TestDb::new();
    db2.add_table_with_indexes("users", &[
        ("id", DataType::I64, false),
        ("name", DataType::String, false),
    ], vec![
        IndexSchema { name: Some("idx_id".into()), columns: vec![0], index_type: IndexType::BTree },
    ]);
    db2.add_table("orders", &[
        ("id", DataType::I64, false),
        ("user_id", DataType::I64, false),
        ("amount", DataType::I64, false),
    ]);
    assert_plan(&db2.plan(
        "SELECT users.name, orders.amount \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         WHERE users.id > 1",
    ), "
Select
  Scan table=users scan=BTree([0] prefix=1 lookup=PrefixRange)
    index_preds: [users.id > 1]
  Join type=Inner strategy=NestedLoop table=orders scan=Full
    on: users.id = orders.user_id
  Output [users.name, orders.amount]
");
}

#[test]
fn not_equals_not_indexable() {
    let db = make_indexed_db();
    assert_plan(&db.plan("SELECT users.name FROM users WHERE users.id != 2"), "
Select
  Scan table=users scan=Full
    pre_filter: users.id != 2
  Output [users.name]
");
}

#[test]
fn and_with_two_indexed_columns_picks_best() {
    let db = make_indexed_db();
    // Both id and age are indexed; optimizer picks the more selective one (id Eq > age range)
    assert_plan(&db.plan(
        "SELECT users.name FROM users WHERE users.id = 2 AND users.age > 20",
    ), "
Select
  Scan table=users scan=BTree([0] prefix=1 lookup=FullKeyEq)
    pre_filter: users.age > 20
    index_preds: [users.id = 2]
  Output [users.name]
");
}

#[test]
fn aggregate_with_limit() {
    let db = make_db();
    assert_plan(&db.plan(
        "SELECT users.name, SUM(users.age) FROM users GROUP BY users.name LIMIT 2",
    ), "
Select
  Scan table=users scan=Full
  GroupBy [users.name]
  Aggregate SUM(users.age)
  Limit 2
  Output [users.name, SUM(users.age)]
");
}

#[test]
fn scalar_subquery_gt() {
    let db = make_db();
    assert_plan(&db.plan(
        "SELECT users.name FROM users \
         WHERE users.age > (SELECT orders.amount FROM orders WHERE orders.id = 12)",
    ), "
Materialize step=0 kind=Scalar
  Scan table=orders scan=Hash([0] prefix=1 lookup=FullKeyEq)
    index_preds: [orders.id = 12]
  Output [orders.amount]
Select
  Scan table=users scan=Full
    pre_filter: users.age > $mat0
  Output [users.name]
");
}

#[test]
fn three_table_join() {
    let mut db = make_db();
    db.add_table("products", &[
        ("id", DataType::I64, false),
        ("name", DataType::String, false),
    ]);
    assert_plan(&db.plan(
        "SELECT users.name, orders.amount, products.name \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         LEFT JOIN products ON orders.id = products.id",
    ), "
Select
  Scan table=users scan=Full
  Join type=Inner strategy=NestedLoop table=orders scan=Full
    on: users.id = orders.user_id
  Join type=Left strategy=IndexLookup(Hash[0]) table=products scan=Full
    on: orders.id = products.id
  Output [users.name, orders.amount, products.name]
");
}

// ── Prepared statement placeholders ────────────────────────────────────

#[test]
fn placeholder_in_filter_and_limit() {
    let db = make_db();
    assert_plan(&db.plan(
        "SELECT users.name FROM users WHERE users.id = :id AND users.age > :min_age LIMIT :n",
    ), "
Select
  Scan table=users scan=Hash([0] prefix=1 lookup=FullKeyEq)
    pre_filter: users.age > :min_age
    index_preds: [users.id = :id]
  Limit :n
  Output [users.name]
");
}

#[test]
fn placeholder_with_index() {
    let db = make_indexed_db();
    assert_plan(&db.plan(
        "SELECT users.name FROM users WHERE users.id = :id",
    ), "
Select
  Scan table=users scan=BTree([0] prefix=1 lookup=FullKeyEq)
    index_preds: [users.id = :id]
  Output [users.name]
");
}

#[test]
fn placeholder_or_to_in_optimization() {
    let db = make_db();
    assert_plan(&db.plan(
        "SELECT users.name FROM users WHERE users.id = :a OR users.id = :b",
    ), "
Select
  Scan table=users scan=Hash([0] prefix=1 lookup=InMultiLookup)
    index_preds: [users.id IN (:a, :b)]
  Output [users.name]
");
}
