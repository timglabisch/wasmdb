use std::collections::HashMap;

use engine_simple::execute::{self, Columns};
use engine_simple::planner;
use engine_simple::storage::{CellValue, Table};
use query_engine::parser;
use query_engine::schema::{ColumnDef, Schema};
use schema_engine::schema::{ColumnSchema, DataType, IndexSchema, IndexType, TableSchema};

// ── Helpers ─────────────────────────────────────────────────────────────────

fn schema_for(name: &str, cols: &[(&str, DataType, bool)]) -> (TableSchema, Schema) {
    let table_schema = TableSchema {
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
    };
    let query_schema = Schema::new(
        cols.iter()
            .map(|(n, _, _)| ColumnDef {
                table: Some(name.into()),
                name: (*n).into(),
            })
            .collect(),
    );
    (table_schema, query_schema)
}

struct TestDb {
    tables: HashMap<String, Table>,
    schemas: HashMap<String, Schema>,
}

impl TestDb {
    fn new() -> Self {
        Self {
            tables: HashMap::new(),
            schemas: HashMap::new(),
        }
    }

    fn add_table(&mut self, name: &str, cols: &[(&str, DataType, bool)]) -> &mut Table {
        let (ts, qs) = schema_for(name, cols);
        self.tables.insert(name.into(), Table::new(ts));
        self.schemas.insert(name.into(), qs);
        self.tables.get_mut(name).unwrap()
    }

    fn add_table_with_indexes(
        &mut self,
        name: &str,
        cols: &[(&str, DataType, bool)],
        indexes: Vec<IndexSchema>,
    ) -> &mut Table {
        let (mut ts, qs) = schema_for(name, cols);
        ts.indexes = indexes;
        self.tables.insert(name.into(), Table::new(ts));
        self.schemas.insert(name.into(), qs);
        self.tables.get_mut(name).unwrap()
    }

    fn run(&self, sql: &str) -> Columns {
        let ast = parser::parse(sql).expect("parse failed");
        let plan = planner::plan_select(&ast, &self.schemas).expect("plan failed");
        execute::execute(&plan, &self.tables).expect("execute failed")
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
