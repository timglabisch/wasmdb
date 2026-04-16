use std::collections::HashMap;

use sql_engine::execute::{self, Columns, ParamValue};
use sql_engine::planner;
use sql_engine::reactive::registry::SubscriptionRegistry;
use sql_engine::storage::{CellValue, Table};
use sql_parser::parser;
use sql_engine::schema::{ColumnSchema, DataType, TableSchema};

// ── Helpers ──────────────────────────────────────────────────────────────

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
}

impl TestDb {
    fn new() -> Self {
        Self {
            tables: HashMap::new(),
            table_schemas: HashMap::new(),
        }
    }

    fn add_table(&mut self, name: &str, cols: &[(&str, DataType, bool)]) -> &mut Table {
        let ts = make_table_schema(name, cols);
        self.table_schemas.insert(name.into(), ts.clone());
        self.tables.insert(name.into(), Table::new(ts));
        self.tables.get_mut(name).unwrap()
    }

    fn run_with_params(&self, sql: &str, params: execute::Params) -> Columns {
        let ast = parser::parse(sql).expect("parse failed");
        let plan = planner::sql::plan(&ast, &self.table_schemas).expect("plan failed");
        let mut ctx = execute::ExecutionContext::with_params(&self.tables, params);
        execute::execute_plan(&mut ctx, &plan).expect("execute failed")
    }
}

fn make_db() -> TestDb {
    let mut db = TestDb::new();
    {
        let t = db.add_table("users", &[
            ("id", DataType::I64, false),
            ("name", DataType::String, false),
            ("age", DataType::I64, false),
        ]);
        t.insert(&[CellValue::I64(1), CellValue::Str("Alice".into()), CellValue::I64(30)]).unwrap();
        t.insert(&[CellValue::I64(2), CellValue::Str("Bob".into()), CellValue::I64(25)]).unwrap();
        t.insert(&[CellValue::I64(3), CellValue::Str("Carol".into()), CellValue::I64(35)]).unwrap();
    }
    {
        let t = db.add_table("orders", &[
            ("id", DataType::I64, false),
            ("user_id", DataType::I64, false),
            ("amount", DataType::I64, false),
        ]);
        t.insert(&[CellValue::I64(1), CellValue::I64(1), CellValue::I64(100)]).unwrap();
        t.insert(&[CellValue::I64(2), CellValue::I64(1), CellValue::I64(200)]).unwrap();
        t.insert(&[CellValue::I64(3), CellValue::I64(2), CellValue::I64(50)]).unwrap();
    }
    db
}

// ── Tests ────────────────────────────────────────────────────────────────

#[test]
fn reactive_pk_watch() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv, users.name FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let ast = parser::parse(sql).expect("parse failed");
    let conditions = sql_engine::reactive::plan_reactive(&ast, &db.table_schemas).expect("plan_reactive failed");
    assert_eq!(conditions.len(), 1);
    assert_eq!(conditions[0].table, "users");

    let mut registry = SubscriptionRegistry::new();
    let sub_id = registry.subscribe(&conditions, &params).unwrap();

    let affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(1), CellValue::Str("NewAlice".into()), CellValue::I64(31)]);
    assert_eq!(affected, vec![sub_id]);

    let affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(99), CellValue::Str("Nobody".into()), CellValue::I64(20)]);
    assert!(affected.is_empty());
}

#[test]
fn reactive_with_verify_filter() {
    let db = make_db();
    let sql = "SELECT REACTIVE(orders.user_id = :uid AND orders.amount > 100) AS inv FROM orders WHERE orders.user_id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let ast = parser::parse(sql).expect("parse failed");
    let conditions = sql_engine::reactive::plan_reactive(&ast, &db.table_schemas).expect("plan_reactive failed");

    let mut registry = SubscriptionRegistry::new();
    let sub_id = registry.subscribe(&conditions, &params).unwrap();

    assert!(sql_engine::reactive::execute::on_insert(&registry, "orders", &[CellValue::I64(10), CellValue::I64(1), CellValue::I64(50)]).is_empty());
    assert_eq!(sql_engine::reactive::execute::on_insert(&registry, "orders", &[CellValue::I64(11), CellValue::I64(1), CellValue::I64(200)]), vec![sub_id]);
    assert!(sql_engine::reactive::execute::on_insert(&registry, "orders", &[CellValue::I64(12), CellValue::I64(99), CellValue::I64(500)]).is_empty());
}

#[test]
fn reactive_unsubscribe() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let ast = parser::parse(sql).expect("parse failed");
    let conditions = sql_engine::reactive::plan_reactive(&ast, &db.table_schemas).expect("plan_reactive failed");

    let mut registry = SubscriptionRegistry::new();
    let sub_id = registry.subscribe(&conditions, &params).unwrap();
    registry.unsubscribe(sub_id);

    assert!(sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(1), CellValue::Str("X".into()), CellValue::I64(1)]).is_empty());
}

#[test]
fn reactive_delete_triggers() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let ast = parser::parse(sql).expect("parse failed");
    let conditions = sql_engine::reactive::plan_reactive(&ast, &db.table_schemas).expect("plan_reactive failed");

    let mut registry = SubscriptionRegistry::new();
    let sub_id = registry.subscribe(&conditions, &params).unwrap();

    assert_eq!(sql_engine::reactive::execute::on_delete(&registry, "users", &[CellValue::I64(1), CellValue::Str("Alice".into()), CellValue::I64(30)]), vec![sub_id]);
}

#[test]
fn reactive_update_leaving_filter() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid AND users.name = 'Alice') AS inv FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let ast = parser::parse(sql).expect("parse failed");
    let conditions = sql_engine::reactive::plan_reactive(&ast, &db.table_schemas).expect("plan_reactive failed");

    let mut registry = SubscriptionRegistry::new();
    let sub_id = registry.subscribe(&conditions, &params).unwrap();

    // UPDATE is represented as delete(old) + insert(new) in a ZSet.
    let mut zset = sql_engine::storage::ZSet::new();
    zset.delete("users".into(), vec![CellValue::I64(1), CellValue::Str("Alice".into()), CellValue::I64(30)]);
    zset.insert("users".into(), vec![CellValue::I64(1), CellValue::Str("Bobby".into()), CellValue::I64(30)]);
    let affected = sql_engine::reactive::execute::on_zset(&registry, &zset);
    assert!(affected.contains_key(&sub_id));
}

#[test]
fn reactive_initial_result_contains_reactive_column() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv, users.name FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let result = db.run_with_params(sql, params);
    assert_eq!(result.len(), 2);
    assert_eq!(result[0], vec![CellValue::I64(0)]);
    assert_eq!(result[1], vec![CellValue::Str("Alice".into())]);
}

#[test]
fn reactive_plan_pretty_print() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv, users.name FROM users WHERE users.id = :uid";
    let ast = parser::parse(sql).expect("parse failed");
    let plan = planner::sql::plan(&ast, &db.table_schemas).expect("plan failed");
    let pp = plan.pretty_print();

    assert!(!pp.contains("Reactive strategy"));
    assert!(pp.contains("REACTIVE[0] AS inv"));
}

#[test]
fn reactive_multi_eq_verify_filter_regression() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid AND users.name = 'Alice') AS inv FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let conditions = sql_engine::reactive::plan_reactive(
        &parser::parse(sql).unwrap(),
        &db.table_schemas,
    ).unwrap();

    let mut registry = SubscriptionRegistry::new();
    let sub_id = registry.subscribe(&conditions, &params).unwrap();

    let affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(1), CellValue::Str("Bob".into()), CellValue::I64(25)]);
    assert!(affected.is_empty(), "should NOT trigger: name mismatch");

    let affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(1), CellValue::Str("Alice".into()), CellValue::I64(30)]);
    assert_eq!(affected, vec![sub_id], "should trigger: full condition matches");

    let affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(99), CellValue::Str("Alice".into()), CellValue::I64(30)]);
    assert!(affected.is_empty(), "should NOT trigger: id mismatch");
}
