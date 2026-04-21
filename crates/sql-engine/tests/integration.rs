use std::collections::HashMap;

use sql_engine::execute::{self, Columns};
use sql_engine::planner;
use sql_engine::planner::requirement::RequirementRegistry;
use sql_engine::storage::{CellValue, Table};
use sql_parser::parser;
use sql_engine::schema::{ColumnSchema, DataType, IndexSchema, IndexType, TableSchema};

// ── Helpers ─────────────────────────────────────────────────────────────────

fn make_table_schema(name: &str, cols: &[(&str, DataType, bool)]) -> TableSchema {
    TableSchema {
        name: name.into(),
        columns: cols
            .iter()
            .map(|(n, dt, nullable)| ColumnSchema {
                name: (*n).into(),
                data_type: *dt,
                nullable: *nullable,
            })
            .collect(),
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
    fn new() -> Self {
        Self {
            tables: HashMap::new(),
            table_schemas: HashMap::new(),
            requirements: RequirementRegistry::new(),
        }
    }

    fn add_table(&mut self, name: &str, cols: &[(&str, DataType, bool)]) -> &mut Table {
        let ts = make_table_schema(name, cols);
        self.table_schemas.insert(name.into(), ts.clone());
        self.tables.insert(name.into(), Table::new(ts));
        self.tables.get_mut(name).unwrap()
    }

    fn add_table_with_indexes(
        &mut self,
        name: &str,
        cols: &[(&str, DataType, bool)],
        indexes: Vec<IndexSchema>,
    ) -> &mut Table {
        let mut ts = make_table_schema(name, cols);
        ts.indexes = indexes;
        self.table_schemas.insert(name.into(), ts.clone());
        self.tables.insert(name.into(), Table::new(ts));
        self.tables.get_mut(name).unwrap()
    }

    fn run(&self, sql: &str) -> Columns {
        let ast = parser::parse(sql).expect("parse failed");
        let plan = planner::sql::plan(&ast, &self.table_schemas, &self.requirements).expect("plan failed");
        let mut ctx = execute::ExecutionContext::new(&self.tables);
        execute::execute_plan(&mut ctx, &plan).expect("execute failed")
    }

    fn run_with_params(&self, sql: &str, params: execute::Params) -> Columns {
        let ast = parser::parse(sql).expect("parse failed");
        let plan = planner::sql::plan(&ast, &self.table_schemas, &self.requirements).expect("plan failed");
        let mut ctx = execute::ExecutionContext::with_params(&self.tables, params);
        execute::execute_plan(&mut ctx, &plan).expect("execute failed")
    }
}

fn i(v: i64) -> CellValue {
    CellValue::I64(v)
}
fn s(v: &str) -> CellValue {
    CellValue::Str(v.into())
}

fn make_db() -> TestDb {
    let mut db = TestDb::new();

    let users = db.add_table(
        "users",
        &[
            ("id", DataType::I64, false),
            ("name", DataType::String, false),
            ("age", DataType::I64, true),
        ],
    );
    users
        .insert(&[i(1), s("Alice"), i(30)])
        .unwrap();
    users
        .insert(&[i(2), s("Bob"), i(25)])
        .unwrap();
    users
        .insert(&[i(3), s("Carol"), i(35)])
        .unwrap();
    users
        .insert(&[i(4), s("Dave"), CellValue::Null])
        .unwrap();

    let orders = db.add_table(
        "orders",
        &[
            ("id", DataType::I64, false),
            ("user_id", DataType::I64, false),
            ("amount", DataType::I64, false),
        ],
    );
    orders
        .insert(&[i(10), i(1), i(100)])
        .unwrap();
    orders
        .insert(&[i(11), i(1), i(200)])
        .unwrap();
    orders
        .insert(&[i(12), i(2), i(50)])
        .unwrap();
    orders
        .insert(&[i(13), i(3), i(300)])
        .unwrap();

    db
}

// ── SELECT * ────────────────────────────────────────────────────────────────

#[test]
fn select_all_columns() {
    let db = make_db();
    let result = db.run("SELECT users.id, users.name, users.age FROM users");
    assert_eq!(result.len(), 3);
    assert_eq!(result[0], vec![i(1), i(2), i(3), i(4)]);
    assert_eq!(result[1], vec![s("Alice"), s("Bob"), s("Carol"), s("Dave")]);
    assert_eq!(result[2], vec![i(30), i(25), i(35), CellValue::Null]);
}

// ── WHERE ───────────────────────────────────────────────────────────────────

#[test]
fn select_where_equals() {
    let db = make_db();
    let result = db.run("SELECT users.name FROM users WHERE users.id = 2");
    assert_eq!(result[0], vec![s("Bob")]);
}

#[test]
fn select_where_greater_than() {
    let db = make_db();
    let result = db.run("SELECT users.name FROM users WHERE users.age > 28");
    assert_eq!(result[0], vec![s("Alice"), s("Carol")]);
}

#[test]
fn select_where_less_than() {
    let db = make_db();
    let result = db.run("SELECT users.name FROM users WHERE users.age < 30");
    assert_eq!(result[0], vec![s("Bob")]);
}

#[test]
fn select_where_and() {
    let db = make_db();
    let result =
        db.run("SELECT users.name FROM users WHERE users.age > 24 AND users.age < 32");
    assert_eq!(result[0], vec![s("Alice"), s("Bob")]);
}

#[test]
fn select_where_or() {
    let db = make_db();
    let result =
        db.run("SELECT users.name FROM users WHERE users.id = 1 OR users.id = 3");
    assert_eq!(result[0], vec![s("Alice"), s("Carol")]);
}

#[test]
fn select_where_no_match() {
    let db = make_db();
    let result = db.run("SELECT users.name FROM users WHERE users.id = 999");
    assert_eq!(result[0].len(), 0);
}

// ── INNER JOIN ──────────────────────────────────────────────────────────────

#[test]
fn inner_join() {
    let db = make_db();
    let result = db.run(
        "SELECT users.name, orders.amount \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id",
    );
    assert_eq!(result[0], vec![s("Alice"), s("Alice"), s("Bob"), s("Carol")]);
    assert_eq!(result[1], vec![i(100), i(200), i(50), i(300)]);
}

#[test]
fn inner_join_with_filter() {
    let db = make_db();
    let result = db.run(
        "SELECT users.name, orders.amount \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         WHERE orders.amount > 100",
    );
    assert_eq!(result[0], vec![s("Alice"), s("Carol")]);
    assert_eq!(result[1], vec![i(200), i(300)]);
}

// ── LEFT JOIN ───────────────────────────────────────────────────────────────

#[test]
fn left_join() {
    let db = make_db();
    let result = db.run(
        "SELECT users.name, orders.amount \
         FROM users \
         LEFT JOIN orders ON users.id = orders.user_id",
    );
    // Alice has 2 orders, Bob has 1, Carol has 1, Dave has 0 → 5 rows
    assert_eq!(result[0].len(), 5);
    assert_eq!(
        result[0],
        vec![s("Alice"), s("Alice"), s("Bob"), s("Carol"), s("Dave")]
    );
    // Dave's order amount should be NULL
    assert_eq!(result[1][4], CellValue::Null);
}

// ── Aggregates ──────────────────────────────────────────────────────────────

#[test]
fn aggregate_count() {
    let db = make_db();
    let result = db.run(
        "SELECT users.name, COUNT(orders.amount) \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         GROUP BY users.name",
    );
    // Alice=2, Bob=1, Carol=1
    assert_eq!(result.len(), 2);
    assert_eq!(result[0], vec![s("Alice"), s("Bob"), s("Carol")]);
    assert_eq!(result[1], vec![i(2), i(1), i(1)]);
}

#[test]
fn aggregate_sum() {
    let db = make_db();
    let result = db.run(
        "SELECT users.name, SUM(orders.amount) \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         GROUP BY users.name",
    );
    assert_eq!(result[0], vec![s("Alice"), s("Bob"), s("Carol")]);
    assert_eq!(result[1], vec![i(300), i(50), i(300)]);
}

#[test]
fn aggregate_min_max() {
    let db = make_db();
    let result = db.run(
        "SELECT users.name, MIN(orders.amount), MAX(orders.amount) \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         GROUP BY users.name",
    );
    assert_eq!(result[0], vec![s("Alice"), s("Bob"), s("Carol")]);
    assert_eq!(result[1], vec![i(100), i(50), i(300)]); // MIN
    assert_eq!(result[2], vec![i(200), i(50), i(300)]); // MAX
}

#[test]
fn aggregate_count_skips_nulls() {
    let db = make_db();
    let result = db.run("SELECT COUNT(users.age) FROM users GROUP BY users.name");
    // Alice=1, Bob=1, Carol=1, Dave=0 (NULL age)
    // group order is insertion order
    assert!(result[0].contains(&i(0)));
}

// ── Single-table aggregates ─────────────────────────────────────────────────

#[test]
fn aggregate_single_table_sum() {
    let db = make_db();
    let result = db.run("SELECT users.name, SUM(users.age) FROM users GROUP BY users.name");
    assert_eq!(result[0], vec![s("Alice"), s("Bob"), s("Carol"), s("Dave")]);
    assert_eq!(result[1], vec![i(30), i(25), i(35), CellValue::Null]);
}

// ── Empty table ─────────────────────────────────────────────────────────────

#[test]
fn select_from_empty_table() {
    let mut db = TestDb::new();
    db.add_table(
        "empty",
        &[
            ("id", DataType::I64, false),
            ("val", DataType::String, false),
        ],
    );
    let result = db.run("SELECT empty.id, empty.val FROM empty");
    assert_eq!(result[0].len(), 0);
    assert_eq!(result[1].len(), 0);
}

// ── Multiple filters on joined data ────────────────────────────────────────

#[test]
fn join_with_multiple_filters() {
    let db = make_db();
    let result = db.run(
        "SELECT users.name, orders.amount \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         WHERE users.age > 24 AND orders.amount > 100",
    );
    // Alice(30) has orders 100,200 → only 200 passes amount>100
    // Bob(25) has order 50 → fails amount>100
    // Carol(35) has order 300 → passes
    assert_eq!(result[0], vec![s("Alice"), s("Carol")]);
    assert_eq!(result[1], vec![i(200), i(300)]);
}

// ── String equality ─────────────────────────────────────────────────────────

#[test]
fn where_string_equals() {
    let db = make_db();
    let result = db.run("SELECT users.id FROM users WHERE users.name = 'Carol'");
    assert_eq!(result[0], vec![i(3)]);
}

// ── Projection reorder ─────────────────────────────────────────────────────

#[test]
fn select_columns_reordered() {
    let db = make_db();
    let result = db.run("SELECT users.age, users.name FROM users WHERE users.id = 1");
    assert_eq!(result[0], vec![i(30)]);
    assert_eq!(result[1], vec![s("Alice")]);
}

// ── ORDER BY ───────────────────────────────────────────────────────────────

#[test]
fn order_by_asc() {
    let db = make_db();
    let result = db.run("SELECT users.name, users.age FROM users ORDER BY users.age");
    assert_eq!(result[0], vec![s("Bob"), s("Alice"), s("Carol"), s("Dave")]);
    assert_eq!(result[1], vec![i(25), i(30), i(35), CellValue::Null]);
}

#[test]
fn order_by_asc_explicit() {
    let db = make_db();
    let result = db.run("SELECT users.name, users.age FROM users ORDER BY users.age ASC");
    assert_eq!(result[0], vec![s("Bob"), s("Alice"), s("Carol"), s("Dave")]);
}

#[test]
fn order_by_desc() {
    let db = make_db();
    let result = db.run("SELECT users.name, users.age FROM users ORDER BY users.age DESC");
    // NULLs first in DESC
    assert_eq!(result[0], vec![s("Dave"), s("Carol"), s("Alice"), s("Bob")]);
}

#[test]
fn order_by_string() {
    let db = make_db();
    let result = db.run("SELECT users.name FROM users ORDER BY users.name");
    assert_eq!(result[0], vec![s("Alice"), s("Bob"), s("Carol"), s("Dave")]);
}

#[test]
fn order_by_with_where() {
    let db = make_db();
    let result = db.run("SELECT users.name FROM users WHERE users.age > 24 ORDER BY users.name DESC");
    assert_eq!(result[0], vec![s("Carol"), s("Bob"), s("Alice")]);
}

#[test]
fn order_by_multiple_keys() {
    let mut db = TestDb::new();
    let t = db.add_table(
        "items",
        &[
            ("id", DataType::I64, false),
            ("category", DataType::I64, false),
            ("name", DataType::String, false),
        ],
    );
    t.insert(&[i(1), i(2), s("Banana")]).unwrap();
    t.insert(&[i(2), i(1), s("Apple")]).unwrap();
    t.insert(&[i(3), i(1), s("Cherry")]).unwrap();
    t.insert(&[i(4), i(2), s("Avocado")]).unwrap();

    let result = db.run("SELECT items.category, items.name FROM items ORDER BY items.category ASC, items.name ASC");
    assert_eq!(result[0], vec![i(1), i(1), i(2), i(2)]);
    assert_eq!(result[1], vec![s("Apple"), s("Cherry"), s("Avocado"), s("Banana")]);
}

#[test]
fn order_by_join() {
    let db = make_db();
    let result = db.run(
        "SELECT users.name, orders.amount \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         ORDER BY orders.amount DESC",
    );
    assert_eq!(result[0], vec![s("Carol"), s("Alice"), s("Alice"), s("Bob")]);
    assert_eq!(result[1], vec![i(300), i(200), i(100), i(50)]);
}

// ── LIMIT ──────────────────────────────────────────────────────────────────

#[test]
fn limit_basic() {
    let db = make_db();
    let result = db.run("SELECT users.name FROM users LIMIT 2");
    assert_eq!(result[0].len(), 2);
    assert_eq!(result[0], vec![s("Alice"), s("Bob")]);
}

#[test]
fn limit_with_order_by() {
    let db = make_db();
    let result = db.run("SELECT users.name, users.age FROM users ORDER BY users.age DESC LIMIT 2");
    assert_eq!(result[0], vec![s("Dave"), s("Carol")]);
}

#[test]
fn limit_larger_than_rows() {
    let db = make_db();
    let result = db.run("SELECT users.name FROM users LIMIT 100");
    assert_eq!(result[0].len(), 4);
}

#[test]
fn limit_zero() {
    let db = make_db();
    let result = db.run("SELECT users.name FROM users LIMIT 0");
    assert_eq!(result[0].len(), 0);
}

#[test]
fn limit_with_where() {
    let db = make_db();
    let result = db.run("SELECT users.name FROM users WHERE users.age > 24 ORDER BY users.name LIMIT 2");
    assert_eq!(result[0], vec![s("Alice"), s("Bob")]);
}

#[test]
fn limit_with_join() {
    let db = make_db();
    let result = db.run(
        "SELECT users.name, orders.amount \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         ORDER BY orders.amount DESC \
         LIMIT 2",
    );
    assert_eq!(result[0], vec![s("Carol"), s("Alice")]);
    assert_eq!(result[1], vec![i(300), i(200)]);
}

#[test]
fn limit_with_aggregate() {
    let db = make_db();
    let result = db.run(
        "SELECT users.name, SUM(orders.amount) \
         FROM users \
         INNER JOIN orders ON users.id = orders.user_id \
         GROUP BY users.name \
         LIMIT 2",
    );
    assert_eq!(result[0].len(), 2);
}

// ── Index-backed queries ──────────────────────────────────────────────────

fn make_indexed_db() -> TestDb {
    let mut db = TestDb::new();

    let users = db.add_table_with_indexes(
        "users",
        &[
            ("id", DataType::I64, false),
            ("name", DataType::String, false),
            ("age", DataType::I64, true),
        ],
        vec![
            IndexSchema { name: Some("idx_id".into()), columns: vec![0], index_type: IndexType::BTree },
            IndexSchema { name: Some("idx_age".into()), columns: vec![2], index_type: IndexType::BTree },
        ],
    );
    users.insert(&[i(1), s("Alice"), i(30)]).unwrap();
    users.insert(&[i(2), s("Bob"), i(25)]).unwrap();
    users.insert(&[i(3), s("Carol"), i(35)]).unwrap();
    users.insert(&[i(4), s("Dave"), CellValue::Null]).unwrap();

    db
}

#[test]
fn index_eq_lookup() {
    let db = make_indexed_db();
    let result = db.run("SELECT users.name FROM users WHERE users.id = 2");
    assert_eq!(result[0], vec![s("Bob")]);
}

#[test]
fn index_range_gt() {
    let db = make_indexed_db();
    let result = db.run("SELECT users.name FROM users WHERE users.age > 28");
    assert_eq!(result[0], vec![s("Alice"), s("Carol")]);
}

#[test]
fn index_range_lt() {
    let db = make_indexed_db();
    let result = db.run("SELECT users.name FROM users WHERE users.age < 30");
    assert_eq!(result[0], vec![s("Bob")]);
}

#[test]
fn index_no_match() {
    let db = make_indexed_db();
    let result = db.run("SELECT users.name FROM users WHERE users.id = 999");
    assert_eq!(result[0].len(), 0);
}

#[test]
fn index_with_order_by() {
    let db = make_indexed_db();
    let result = db.run("SELECT users.name FROM users WHERE users.age > 24 ORDER BY users.name");
    assert_eq!(result[0], vec![s("Alice"), s("Bob"), s("Carol")]);
}

#[test]
fn index_with_limit() {
    let db = make_indexed_db();
    let result = db.run("SELECT users.name FROM users WHERE users.id > 0 ORDER BY users.name LIMIT 2");
    assert_eq!(result[0], vec![s("Alice"), s("Bob")]);
}

// ── Composite index queries ──────────────────────────────────────────────

fn make_composite_indexed_db() -> TestDb {
    let mut db = TestDb::new();

    let events = db.add_table_with_indexes(
        "events",
        &[
            ("user_id", DataType::I64, false),
            ("category", DataType::I64, false),
            ("score", DataType::I64, false),
        ],
        vec![
            IndexSchema {
                name: Some("idx_user_cat".into()),
                columns: vec![0, 1],
                index_type: IndexType::BTree,
            },
        ],
    );
    // (user_id, category, score)
    events.insert(&[i(1), i(10), i(100)]).unwrap();
    events.insert(&[i(1), i(20), i(200)]).unwrap();
    events.insert(&[i(2), i(10), i(300)]).unwrap();
    events.insert(&[i(2), i(20), i(400)]).unwrap();
    events.insert(&[i(2), i(30), i(500)]).unwrap();

    db
}

#[test]
fn composite_index_full_eq() {
    let db = make_composite_indexed_db();
    let result = db.run(
        "SELECT events.score FROM events WHERE events.user_id = 2 AND events.category = 20",
    );
    assert_eq!(result[0], vec![i(400)]);
}

#[test]
fn composite_index_prefix_eq() {
    let db = make_composite_indexed_db();
    let result = db.run(
        "SELECT events.score FROM events WHERE events.user_id = 1",
    );
    assert_eq!(result[0], vec![i(100), i(200)]);
}

#[test]
fn composite_index_prefix_eq_with_range() {
    let db = make_composite_indexed_db();
    let result = db.run(
        "SELECT events.score FROM events WHERE events.user_id = 2 AND events.category > 10",
    );
    assert_eq!(result[0], vec![i(400), i(500)]);
}

#[test]
fn composite_index_prefix_eq_with_range_lt() {
    let db = make_composite_indexed_db();
    let result = db.run(
        "SELECT events.score FROM events WHERE events.user_id = 2 AND events.category < 30",
    );
    assert_eq!(result[0], vec![i(300), i(400)]);
}

#[test]
fn composite_index_with_remaining_filter() {
    let db = make_composite_indexed_db();
    // Index covers (user_id, category), score filter applied as post-filter.
    let result = db.run(
        "SELECT events.score FROM events WHERE events.user_id = 2 AND events.category >= 10 AND events.score > 350",
    );
    assert_eq!(result[0], vec![i(400), i(500)]);
}

#[test]
fn composite_index_no_prefix_falls_back() {
    let db = make_composite_indexed_db();
    // category-only filter can't use the (user_id, category) index — falls back to scan.
    let result = db.run(
        "SELECT events.score FROM events WHERE events.category = 10",
    );
    assert_eq!(result[0], vec![i(100), i(300)]);
}

#[test]
fn composite_index_with_order_by() {
    let db = make_composite_indexed_db();
    let result = db.run(
        "SELECT events.category, events.score FROM events WHERE events.user_id = 2 ORDER BY events.score DESC",
    );
    assert_eq!(result[0], vec![i(30), i(20), i(10)]);
    assert_eq!(result[1], vec![i(500), i(400), i(300)]);
}

// ── IN queries ────────────────────────────────────────────────────────────

#[test]
fn in_literal_list() {
    let db = make_db();
    let result = db.run("SELECT users.name FROM users WHERE users.id IN (1, 3)");
    assert_eq!(result[0], vec![s("Alice"), s("Carol")]);
}

#[test]
fn in_literal_strings() {
    let db = make_db();
    let result = db.run("SELECT users.id FROM users WHERE users.name IN ('Bob', 'Dave')");
    assert_eq!(result[0], vec![i(2), i(4)]);
}

#[test]
fn in_literal_no_match() {
    let db = make_db();
    let result = db.run("SELECT users.name FROM users WHERE users.id IN (99, 100)");
    assert_eq!(result[0].len(), 0);
}

#[test]
fn in_literal_with_and() {
    let db = make_db();
    let result = db.run(
        "SELECT users.name FROM users WHERE users.id IN (1, 2, 3) AND users.age > 28",
    );
    assert_eq!(result[0], vec![s("Alice"), s("Carol")]);
}

#[test]
fn in_literal_with_order_by() {
    let db = make_db();
    let result = db.run(
        "SELECT users.name FROM users WHERE users.id IN (3, 1) ORDER BY users.name",
    );
    assert_eq!(result[0], vec![s("Alice"), s("Carol")]);
}

#[test]
fn in_subquery() {
    let db = make_db();
    // Users who have orders with amount > 100
    let result = db.run(
        "SELECT users.name FROM users WHERE users.id IN (SELECT orders.user_id FROM orders WHERE orders.amount > 100)",
    );
    // orders with amount > 100: (11, user_id=1, 200), (13, user_id=3, 300)
    assert_eq!(result[0], vec![s("Alice"), s("Carol")]);
}

#[test]
fn in_subquery_empty() {
    let db = make_db();
    let result = db.run(
        "SELECT users.name FROM users WHERE users.id IN (SELECT orders.user_id FROM orders WHERE orders.amount > 9999)",
    );
    assert_eq!(result[0].len(), 0);
}

#[test]
fn scalar_subquery_comparison() {
    let db = make_db();
    // orders.id=12 has amount=50, users with age > 50: none
    let result = db.run(
        "SELECT users.name FROM users WHERE users.age > (SELECT orders.amount FROM orders WHERE orders.id = 12)",
    );
    assert_eq!(result[0].len(), 0);

    // orders.id=10 has amount=100, users with age > 100: none
    let result2 = db.run(
        "SELECT users.name FROM users WHERE users.age > (SELECT orders.amount FROM orders WHERE orders.id = 10)",
    );
    assert_eq!(result2[0].len(), 0);
}

#[test]
fn scalar_subquery_eq() {
    let db = make_db();
    // Find user whose age equals the amount of order 12 (amount=50)
    let result = db.run(
        "SELECT users.name FROM users WHERE users.age = (SELECT orders.amount FROM orders WHERE orders.id = 12)",
    );
    // amount=50, no user has age 50
    assert_eq!(result[0].len(), 0);

    // Find user whose id equals user_id of order 12 (user_id=2)
    let result2 = db.run(
        "SELECT users.name FROM users WHERE users.id = (SELECT orders.user_id FROM orders WHERE orders.id = 12)",
    );
    assert_eq!(result2[0], vec![s("Bob")]);
}

#[test]
fn in_with_index() {
    let db = make_indexed_db();
    let result = db.run("SELECT users.name FROM users WHERE users.id IN (1, 3)");
    assert_eq!(result[0], vec![s("Alice"), s("Carol")]);
}

// ── Prepared statements (named placeholders) ──────────────────────────────

#[test]
fn prepared_scalar_int() {
    let db = make_db();
    let params = HashMap::from([("id".into(), execute::ParamValue::Int(2))]);
    let result = db.run_with_params(
        "SELECT users.name FROM users WHERE users.id = :id",
        params,
    );
    assert_eq!(result[0], vec![s("Bob")]);
}

#[test]
fn prepared_scalar_text() {
    let db = make_db();
    let params = HashMap::from([("name".into(), execute::ParamValue::Text("Alice".into()))]);
    let result = db.run_with_params(
        "SELECT users.id FROM users WHERE users.name = :name",
        params,
    );
    assert_eq!(result[0], vec![i(1)]);
}

#[test]
fn prepared_in_int_list() {
    let db = make_db();
    let params = HashMap::from([("ids".into(), execute::ParamValue::IntList(vec![1, 3]))]);
    let result = db.run_with_params(
        "SELECT users.name FROM users WHERE users.id IN (:ids)",
        params,
    );
    assert_eq!(result[0], vec![s("Alice"), s("Carol")]);
}

#[test]
fn prepared_in_text_list() {
    let db = make_db();
    let params = HashMap::from([("names".into(), execute::ParamValue::TextList(vec!["Alice".into(), "Carol".into()]))]);
    let result = db.run_with_params(
        "SELECT users.id FROM users WHERE users.name IN (:names)",
        params,
    );
    assert_eq!(result[0], vec![i(1), i(3)]);
}

#[test]
fn prepared_multiple_params() {
    let db = make_db();
    let params = HashMap::from([
        ("min_age".into(), execute::ParamValue::Int(25)),
        ("name".into(), execute::ParamValue::Text("Carol".into())),
    ]);
    let result = db.run_with_params(
        "SELECT users.name FROM users WHERE users.age > :min_age AND users.name = :name",
        params,
    );
    assert_eq!(result[0], vec![s("Carol")]);
}

#[test]
fn prepared_reuse_plan_different_params() {
    let db = make_db();
    let sql = "SELECT users.name FROM users WHERE users.id = :id";
    let ast = parser::parse(sql).unwrap();
    let plan = planner::sql::plan(&ast, &db.table_schemas, &db.requirements).unwrap();

    // First execution
    let mut ctx1 = execute::ExecutionContext::with_params(
        &db.tables,
        HashMap::from([("id".into(), execute::ParamValue::Int(1))]),
    );
    let r1 = execute::execute_plan(&mut ctx1, &plan).unwrap();
    assert_eq!(r1[0], vec![s("Alice")]);

    // Second execution with different params — same plan
    let mut ctx2 = execute::ExecutionContext::with_params(
        &db.tables,
        HashMap::from([("id".into(), execute::ParamValue::Int(3))]),
    );
    let r2 = execute::execute_plan(&mut ctx2, &plan).unwrap();
    assert_eq!(r2[0], vec![s("Carol")]);
}

#[test]
fn prepared_limit_placeholder() {
    let db = make_db();
    let params = HashMap::from([("n".into(), execute::ParamValue::Int(2))]);
    let result = db.run_with_params(
        "SELECT users.name FROM users LIMIT :n",
        params,
    );
    assert_eq!(result[0].len(), 2);
}

#[test]
fn prepared_with_index() {
    let db = make_indexed_db();
    let params = HashMap::from([("id".into(), execute::ParamValue::Int(2))]);
    let result = db.run_with_params(
        "SELECT users.name FROM users WHERE users.id = :id",
        params,
    );
    assert_eq!(result[0], vec![s("Bob")]);
}

#[test]
fn prepared_or_to_in() {
    let db = make_db();
    let params = HashMap::from([
        ("a".into(), execute::ParamValue::Int(1)),
        ("b".into(), execute::ParamValue::Int(3)),
    ]);
    let result = db.run_with_params(
        "SELECT users.name FROM users WHERE users.id = :a OR users.id = :b",
        params,
    );
    assert_eq!(result[0], vec![s("Alice"), s("Carol")]);
}

#[test]
fn prepared_null_parameter() {
    let db = make_db();
    let params = HashMap::from([("val".into(), execute::ParamValue::Null)]);
    let result = db.run_with_params(
        "SELECT users.name FROM users WHERE users.id = :val",
        params,
    );
    assert!(result[0].is_empty());
}

#[test]
fn prepared_empty_int_list() {
    let db = make_db();
    let params = HashMap::from([("ids".into(), execute::ParamValue::IntList(vec![]))]);
    let result = db.run_with_params(
        "SELECT users.name FROM users WHERE users.id IN (:ids)",
        params,
    );
    assert!(result[0].is_empty());
}

#[test]
fn prepared_placeholder_on_left() {
    let db = make_db();
    let params = HashMap::from([("min_age".into(), execute::ParamValue::Int(28))]);
    let result = db.run_with_params(
        "SELECT users.name FROM users WHERE :min_age < users.age",
        params,
    );
    assert_eq!(result[0], vec![s("Alice"), s("Carol")]);
}

#[test]
fn prepared_join_on_placeholder() {
    let db = make_db();
    let params = HashMap::from([("uid".into(), execute::ParamValue::Int(1))]);
    let result = db.run_with_params(
        "SELECT users.name FROM users JOIN orders ON users.id = :uid",
        params,
    );
    // users.id = 1 matches Alice; cross-joined with all 4 orders → 4 rows of "Alice"
    assert_eq!(result[0], vec![s("Alice"), s("Alice"), s("Alice"), s("Alice")]);
}

#[test]
fn prepared_limit_with_placeholder() {
    let db = make_db();
    let params = HashMap::from([("n".into(), execute::ParamValue::Int(1))]);
    let result = db.run_with_params(
        "SELECT users.name FROM users LIMIT :n",
        params,
    );
    assert_eq!(result[0].len(), 1);
}

// ── Caller-backed FROM sources (end-to-end P3 → P6) ─────────────────────────

#[test]
fn caller_source_end_to_end() {
    use sql_engine::planner::requirement::{RequirementMeta, RequirementParamDef};

    let mut db = make_db();
    // Register a caller `users.by_owner` that takes one I64 param and feeds
    // rows into the `users` row_table.
    db.requirements.insert(
        "users::by_owner".into(),
        RequirementMeta {
            row_table: "users".into(),
            params: vec![RequirementParamDef {
                name: "owner_id".into(),
                data_type: DataType::I64,
            }],
        },
    );

    // Plan the SQL query. The P3 translator auto-platzhalterisiert the
    // literal `2` as `__caller_0_arg_0` and stashes the Int(2) in bound_values.
    let ast = parser::parse("SELECT users.id, users.name FROM users.by_owner(2)")
        .expect("parse");
    let plan = planner::sql::plan(&ast, &db.table_schemas, &db.requirements)
        .expect("plan");

    // Ensure bound_values flowed through the planner.
    assert_eq!(plan.bound_values.len(), 1);
    assert!(plan.bound_values.contains_key("__caller_0_arg_0"));

    let mut ctx = execute::ExecutionContext::new(&db.tables);
    ctx.callers.insert(
        "users::by_owner".into(),
        Box::new(|args: &[sql_parser::ast::Value]| {
            // Simulate: owner=2 returns users with id 2 and 3 (Bob, Carol).
            let owner = match args.first() {
                Some(sql_parser::ast::Value::Int(n)) => *n,
                _ => return Err("expected Int owner".into()),
            };
            if owner == 2 {
                Ok(vec![
                    vec![sql_parser::ast::Value::Int(2)],
                    vec![sql_parser::ast::Value::Int(3)],
                ])
            } else {
                Ok(vec![])
            }
        }),
    );

    let result = execute::execute_plan(&mut ctx, &plan).expect("execute");
    assert_eq!(result[0], vec![i(2), i(3)]);
    assert_eq!(result[1], vec![s("Bob"), s("Carol")]);
}

#[test]
fn caller_source_with_user_placeholder() {
    use sql_engine::planner::requirement::{RequirementMeta, RequirementParamDef};

    let mut db = make_db();
    db.requirements.insert(
        "users::by_owner".into(),
        RequirementMeta {
            row_table: "users".into(),
            params: vec![RequirementParamDef {
                name: "owner_id".into(),
                data_type: DataType::I64,
            }],
        },
    );

    // The `:owner` is a user-supplied placeholder — not auto-platzhalterisiert.
    // It stays as a pass-through `RequirementArg::Placeholder("owner")` and
    // must resolve from ctx.params at execute-time.
    let ast = parser::parse("SELECT users.id, users.name FROM users.by_owner(:owner)")
        .expect("parse");
    let plan = planner::sql::plan(&ast, &db.table_schemas, &db.requirements)
        .expect("plan");

    // No auto-bound_values for a user placeholder.
    assert!(plan.bound_values.is_empty());

    let params = HashMap::from([(
        "owner".into(),
        execute::ParamValue::Int(1),
    )]);
    let mut ctx = execute::ExecutionContext::with_params(&db.tables, params);
    ctx.callers.insert(
        "users::by_owner".into(),
        Box::new(|args: &[sql_parser::ast::Value]| {
            let owner = match args.first() {
                Some(sql_parser::ast::Value::Int(n)) => *n,
                _ => return Err("expected Int".into()),
            };
            assert_eq!(owner, 1, "caller should receive resolved user-param value");
            Ok(vec![vec![sql_parser::ast::Value::Int(1)]])
        }),
    );

    let result = execute::execute_plan(&mut ctx, &plan).expect("execute");
    assert_eq!(result[0], vec![i(1)]);
    assert_eq!(result[1], vec![s("Alice")]);
}

// ── Caller-backed FROM sources — more literal + operator coverage ───────────

/// Helper: register a users::by_owner caller that returns the echo of its
/// single I64 arg (and that id + 2 if present). Reused across tests below.
fn register_users_by_owner(ctx: &mut execute::ExecutionContext) {
    ctx.callers.insert(
        "users::by_owner".into(),
        Box::new(|args: &[sql_parser::ast::Value]| {
            let owner = match args.first() {
                Some(sql_parser::ast::Value::Int(n)) => *n,
                other => return Err(format!("expected Int, got {other:?}")),
            };
            let mut result = vec![vec![sql_parser::ast::Value::Int(owner)]];
            if owner + 1 <= 4 {
                result.push(vec![sql_parser::ast::Value::Int(owner + 1)]);
            }
            Ok(result)
        }),
    );
}

fn register_users_meta(db: &mut TestDb) {
    use sql_engine::planner::requirement::{RequirementMeta, RequirementParamDef};
    db.requirements.insert(
        "users::by_owner".into(),
        RequirementMeta {
            row_table: "users".into(),
            params: vec![RequirementParamDef {
                name: "owner_id".into(),
                data_type: DataType::I64,
            }],
        },
    );
}

#[test]
fn caller_source_with_text_literal_arg() {
    use sql_engine::planner::requirement::{RequirementMeta, RequirementParamDef};

    let mut db = make_db();
    db.requirements.insert(
        "users::by_name".into(),
        RequirementMeta {
            row_table: "users".into(),
            params: vec![RequirementParamDef {
                name: "name".into(),
                data_type: DataType::String,
            }],
        },
    );

    let ast = parser::parse("SELECT users.id FROM users.by_name('Bob')")
        .expect("parse");
    let plan = planner::sql::plan(&ast, &db.table_schemas, &db.requirements)
        .expect("plan");

    let mut ctx = execute::ExecutionContext::new(&db.tables);
    ctx.callers.insert(
        "users::by_name".into(),
        Box::new(|args: &[sql_parser::ast::Value]| {
            match args.first() {
                Some(sql_parser::ast::Value::Text(n)) if n == "Bob" =>
                    Ok(vec![vec![sql_parser::ast::Value::Int(2)]]),
                other => Err(format!("expected Text(Bob), got {other:?}")),
            }
        }),
    );

    let result = execute::execute_plan(&mut ctx, &plan).expect("execute");
    assert_eq!(result[0], vec![i(2)]);
}

#[test]
fn caller_source_with_null_literal_arg() {
    let mut db = make_db();
    register_users_meta(&mut db);

    let ast = parser::parse("SELECT users.id FROM users.by_owner(NULL)")
        .expect("parse");
    let plan = planner::sql::plan(&ast, &db.table_schemas, &db.requirements)
        .expect("plan");

    let mut ctx = execute::ExecutionContext::new(&db.tables);
    ctx.callers.insert(
        "users::by_owner".into(),
        Box::new(|args: &[sql_parser::ast::Value]| {
            match args.first() {
                Some(sql_parser::ast::Value::Null) =>
                    Ok(vec![vec![sql_parser::ast::Value::Int(1)]]),
                other => Err(format!("expected Null, got {other:?}")),
            }
        }),
    );

    let result = execute::execute_plan(&mut ctx, &plan).expect("execute");
    assert_eq!(result[0], vec![i(1)]);
}

#[test]
fn caller_source_with_multiple_literal_args() {
    use sql_engine::planner::requirement::{RequirementMeta, RequirementParamDef};

    let mut db = make_db();
    db.requirements.insert(
        "users::by_age_range".into(),
        RequirementMeta {
            row_table: "users".into(),
            params: vec![
                RequirementParamDef { name: "min_age".into(), data_type: DataType::I64 },
                RequirementParamDef { name: "max_age".into(), data_type: DataType::I64 },
            ],
        },
    );

    let ast = parser::parse(
        "SELECT users.id, users.age FROM users.by_age_range(25, 32)"
    ).expect("parse");
    let plan = planner::sql::plan(&ast, &db.table_schemas, &db.requirements)
        .expect("plan");

    // Both literals auto-platzhalterisiert.
    assert_eq!(plan.bound_values.len(), 2);
    assert!(plan.bound_values.contains_key("__caller_0_arg_0"));
    assert!(plan.bound_values.contains_key("__caller_0_arg_1"));

    let mut ctx = execute::ExecutionContext::new(&db.tables);
    ctx.callers.insert(
        "users::by_age_range".into(),
        Box::new(|args: &[sql_parser::ast::Value]| {
            assert_eq!(args.len(), 2);
            match (&args[0], &args[1]) {
                (sql_parser::ast::Value::Int(25), sql_parser::ast::Value::Int(32)) =>
                    Ok(vec![vec![sql_parser::ast::Value::Int(1)], vec![sql_parser::ast::Value::Int(2)]]),
                other => Err(format!("bad args: {other:?}")),
            }
        }),
    );

    let result = execute::execute_plan(&mut ctx, &plan).expect("execute");
    assert_eq!(result[0], vec![i(1), i(2)]);
    assert_eq!(result[1], vec![i(30), i(25)]);
}

#[test]
fn caller_source_with_where_clause() {
    let mut db = make_db();
    register_users_meta(&mut db);

    let ast = parser::parse(
        "SELECT users.id, users.age FROM users.by_owner(1) WHERE users.age > 28"
    ).expect("parse");
    let plan = planner::sql::plan(&ast, &db.table_schemas, &db.requirements)
        .expect("plan");

    let mut ctx = execute::ExecutionContext::new(&db.tables);
    ctx.callers.insert(
        "users::by_owner".into(),
        Box::new(|_| Ok(vec![
            vec![sql_parser::ast::Value::Int(1)], // Alice, age 30
            vec![sql_parser::ast::Value::Int(2)], // Bob, age 25 (filtered out)
            vec![sql_parser::ast::Value::Int(3)], // Carol, age 35
        ])),
    );
    let result = execute::execute_plan(&mut ctx, &plan).expect("execute");
    assert_eq!(result[0], vec![i(1), i(3)]);
    assert_eq!(result[1], vec![i(30), i(35)]);
}

#[test]
fn caller_source_with_order_by() {
    let mut db = make_db();
    register_users_meta(&mut db);

    let ast = parser::parse(
        "SELECT users.name FROM users.by_owner(1) ORDER BY users.age DESC"
    ).expect("parse");
    let plan = planner::sql::plan(&ast, &db.table_schemas, &db.requirements)
        .expect("plan");

    let mut ctx = execute::ExecutionContext::new(&db.tables);
    ctx.callers.insert(
        "users::by_owner".into(),
        Box::new(|_| Ok(vec![
            vec![sql_parser::ast::Value::Int(1)], // Alice 30
            vec![sql_parser::ast::Value::Int(2)], // Bob 25
            vec![sql_parser::ast::Value::Int(3)], // Carol 35
        ])),
    );
    let result = execute::execute_plan(&mut ctx, &plan).expect("execute");
    assert_eq!(result[0], vec![s("Carol"), s("Alice"), s("Bob")]);
}

#[test]
fn caller_source_with_limit() {
    let mut db = make_db();
    register_users_meta(&mut db);

    let ast = parser::parse(
        "SELECT users.id FROM users.by_owner(1) ORDER BY users.id ASC LIMIT 2"
    ).expect("parse");
    let plan = planner::sql::plan(&ast, &db.table_schemas, &db.requirements)
        .expect("plan");

    let mut ctx = execute::ExecutionContext::new(&db.tables);
    ctx.callers.insert(
        "users::by_owner".into(),
        Box::new(|_| Ok(vec![
            vec![sql_parser::ast::Value::Int(3)],
            vec![sql_parser::ast::Value::Int(1)],
            vec![sql_parser::ast::Value::Int(2)],
        ])),
    );
    let result = execute::execute_plan(&mut ctx, &plan).expect("execute");
    assert_eq!(result[0], vec![i(1), i(2)]);
}

#[test]
fn caller_source_with_group_by_and_aggregate() {
    // Caller returns users; JOIN orders; GROUP BY age-bucket; COUNT orders.
    // Covers Phase 2 join + Phase 4 aggregate on a caller-seeded scan.
    let mut db = make_db();
    register_users_meta(&mut db);

    let ast = parser::parse(
        "SELECT users.name, COUNT(orders.amount) \
         FROM users.by_owner(1) \
         INNER JOIN orders ON users.id = orders.user_id \
         GROUP BY users.name"
    ).expect("parse");
    let plan = planner::sql::plan(&ast, &db.table_schemas, &db.requirements)
        .expect("plan");

    let mut ctx = execute::ExecutionContext::new(&db.tables);
    register_users_by_owner(&mut ctx);

    let result = execute::execute_plan(&mut ctx, &plan).expect("execute");
    // by_owner(1) returns [1, 2] → Alice(2 orders) + Bob(1 order).
    // Row order may depend on HashMap iteration of group keys, so sort.
    let mut pairs: Vec<(CellValue, CellValue)> = result[0].iter()
        .cloned()
        .zip(result[1].iter().cloned())
        .collect();
    pairs.sort_by_key(|(n, _)| match n {
        CellValue::Str(s) => s.clone(),
        _ => String::new(),
    });
    assert_eq!(pairs, vec![
        (s("Alice"), i(2)),
        (s("Bob"), i(1)),
    ]);
}

#[test]
fn caller_source_joined_with_plain_table_end_to_end() {
    // Caller first source + INNER JOIN orders via SQL. Covers Phase 2 executor
    // path for Table joined onto a caller-seeded first RowSet.
    let mut db = make_db();
    register_users_meta(&mut db);

    let ast = parser::parse(
        "SELECT users.name, orders.amount \
         FROM users.by_owner(1) \
         INNER JOIN orders ON users.id = orders.user_id \
         ORDER BY orders.amount ASC"
    ).expect("parse");
    let plan = planner::sql::plan(&ast, &db.table_schemas, &db.requirements)
        .expect("plan");

    let mut ctx = execute::ExecutionContext::new(&db.tables);
    register_users_by_owner(&mut ctx);
    let result = execute::execute_plan(&mut ctx, &plan).expect("execute");
    // Alice(1) → 100, 200; Bob(2) → 50.
    assert_eq!(result[0], vec![s("Bob"), s("Alice"), s("Alice")]);
    assert_eq!(result[1], vec![i(50), i(100), i(200)]);
}

#[test]
fn caller_source_unregistered_caller_runtime_errors() {
    // Planner passes (registry has Meta) but no runtime closure is registered.
    let mut db = make_db();
    register_users_meta(&mut db);

    let ast = parser::parse("SELECT users.id FROM users.by_owner(1)").expect("parse");
    let plan = planner::sql::plan(&ast, &db.table_schemas, &db.requirements).expect("plan");

    let mut ctx = execute::ExecutionContext::new(&db.tables);
    // deliberately no ctx.callers.insert(...)
    let err = execute::execute_plan(&mut ctx, &plan).unwrap_err();
    match err {
        execute::ExecuteError::CallerError(msg) => {
            assert!(msg.contains("users::by_owner"), "got: {msg}");
            assert!(msg.contains("not registered"), "got: {msg}");
        }
        other => panic!("expected CallerError, got {other:?}"),
    }
}
