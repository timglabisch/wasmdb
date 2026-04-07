use std::collections::HashMap;

use sql_engine::execute::{self, Columns, ParamValue};
use sql_engine::planner;
use sql_engine::reactive::SubscriptionRegistry;
use sql_engine::storage::{CellValue, Table};
use sql_parser::parser;
use ddl_parser::schema::{ColumnSchema, DataType, TableSchema};

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
        let plan = planner::plan(&ast, &self.table_schemas).expect("plan failed");
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
    let sql = "SELECT INVALIDATE_ON(users.id = :uid) AS inv, users.name FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let ast = parser::parse(sql).expect("parse failed");
    let plan = planner::plan(&ast, &db.table_schemas).expect("plan failed");

    // Plan should have reactive metadata
    assert!(plan.reactive.is_some());
    let reactive = plan.reactive.as_ref().unwrap();
    assert_eq!(reactive.conditions.len(), 1);
    assert_eq!(reactive.conditions[0].table, "users");

    // Execute and subscribe
    let mut ctx = execute::ExecutionContext::with_params(&db.tables, params.clone());
    let result = execute::execute_plan(&mut ctx, &plan).expect("execute failed");

    let mut registry = SubscriptionRegistry::new();
    // Resolve params before subscribing
    let resolved = execute::resolve_params(
        &plan.main,
        &params,
    ).unwrap();
    let _ = resolved; // We need the full plan resolved
    let resolved_plan = sql_engine::execute::bind::resolve_plan_params(&plan, &params).unwrap();
    let sub_id = registry.subscribe(&resolved_plan, &params, result);

    // INSERT matching row → affected
    let affected = registry.on_insert("users", &[CellValue::I64(1), CellValue::Str("NewAlice".into()), CellValue::I64(31)]);
    assert_eq!(affected, vec![sub_id]);

    // INSERT non-matching row → not affected
    let affected = registry.on_insert("users", &[CellValue::I64(99), CellValue::Str("Nobody".into()), CellValue::I64(20)]);
    assert!(affected.is_empty());
}

#[test]
fn reactive_with_verify_filter() {
    let db = make_db();
    let sql = "SELECT INVALIDATE_ON(orders.user_id = :uid AND orders.amount > 100) AS inv FROM orders WHERE orders.user_id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let ast = parser::parse(sql).expect("parse failed");
    let plan = planner::plan(&ast, &db.table_schemas).expect("plan failed");

    let mut ctx = execute::ExecutionContext::with_params(&db.tables, params.clone());
    let result = execute::execute_plan(&mut ctx, &plan).expect("execute failed");

    let mut registry = SubscriptionRegistry::new();
    let resolved_plan = sql_engine::execute::bind::resolve_plan_params(&plan, &params).unwrap();
    let sub_id = registry.subscribe(&resolved_plan, &params, result);

    // amount=50 → verify filter fails
    let affected = registry.on_insert("orders", &[CellValue::I64(10), CellValue::I64(1), CellValue::I64(50)]);
    assert!(affected.is_empty());

    // amount=200 → verify filter passes
    let affected = registry.on_insert("orders", &[CellValue::I64(11), CellValue::I64(1), CellValue::I64(200)]);
    assert_eq!(affected, vec![sub_id]);

    // different user_id → not affected
    let affected = registry.on_insert("orders", &[CellValue::I64(12), CellValue::I64(99), CellValue::I64(500)]);
    assert!(affected.is_empty());
}

#[test]
fn reactive_unsubscribe() {
    let db = make_db();
    let sql = "SELECT INVALIDATE_ON(users.id = :uid) AS inv FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let ast = parser::parse(sql).expect("parse failed");
    let plan = planner::plan(&ast, &db.table_schemas).expect("plan failed");

    let mut ctx = execute::ExecutionContext::with_params(&db.tables, params.clone());
    let result = execute::execute_plan(&mut ctx, &plan).expect("execute failed");

    let mut registry = SubscriptionRegistry::new();
    let resolved_plan = sql_engine::execute::bind::resolve_plan_params(&plan, &params).unwrap();
    let sub_id = registry.subscribe(&resolved_plan, &params, result);
    registry.unsubscribe(sub_id);

    let affected = registry.on_insert("users", &[CellValue::I64(1), CellValue::Str("X".into()), CellValue::I64(1)]);
    assert!(affected.is_empty());
}

#[test]
fn reactive_delete_triggers() {
    let db = make_db();
    let sql = "SELECT INVALIDATE_ON(users.id = :uid) AS inv FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let ast = parser::parse(sql).expect("parse failed");
    let plan = planner::plan(&ast, &db.table_schemas).expect("plan failed");

    let mut ctx = execute::ExecutionContext::with_params(&db.tables, params.clone());
    let result = execute::execute_plan(&mut ctx, &plan).expect("execute failed");

    let mut registry = SubscriptionRegistry::new();
    let resolved_plan = sql_engine::execute::bind::resolve_plan_params(&plan, &params).unwrap();
    let sub_id = registry.subscribe(&resolved_plan, &params, result);

    let affected = registry.on_delete("users", &[CellValue::I64(1), CellValue::Str("Alice".into()), CellValue::I64(30)]);
    assert_eq!(affected, vec![sub_id]);
}

#[test]
fn reactive_update_leaving_filter() {
    let db = make_db();
    // Watch user 1 with name = 'Alice'
    let sql = "SELECT INVALIDATE_ON(users.id = :uid AND users.name = 'Alice') AS inv FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let ast = parser::parse(sql).expect("parse failed");
    let plan = planner::plan(&ast, &db.table_schemas).expect("plan failed");

    let mut ctx = execute::ExecutionContext::with_params(&db.tables, params.clone());
    let result = execute::execute_plan(&mut ctx, &plan).expect("execute failed");

    let mut registry = SubscriptionRegistry::new();
    let resolved_plan = sql_engine::execute::bind::resolve_plan_params(&plan, &params).unwrap();
    let sub_id = registry.subscribe(&resolved_plan, &params, result);

    // UPDATE: name changes from 'Alice' to 'Bobby'
    // Old row matches (name='Alice'), new row doesn't → still affected
    let affected = registry.on_update(
        "users",
        &[CellValue::I64(1), CellValue::Str("Alice".into()), CellValue::I64(30)],
        &[CellValue::I64(1), CellValue::Str("Bobby".into()), CellValue::I64(30)],
    );
    assert!(affected.contains(&sub_id));
}

#[test]
fn reactive_initial_result_contains_invalidate_on_column() {
    let db = make_db();
    let sql = "SELECT INVALIDATE_ON(users.id = :uid) AS inv, users.name FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let result = db.run_with_params(sql, params);
    // Should have 2 columns: INVALIDATE_ON (always 0) + users.name
    assert_eq!(result.len(), 2);
    // INVALIDATE_ON column should be 0 for all rows
    assert_eq!(result[0], vec![CellValue::I64(0)]);
    // users.name should be "Alice"
    assert_eq!(result[1], vec![CellValue::Str("Alice".into())]);
}

#[test]
fn reactive_plan_pretty_print() {
    let db = make_db();
    let sql = "SELECT INVALIDATE_ON(users.id = :uid) AS inv, users.name FROM users WHERE users.id = :uid";
    let ast = parser::parse(sql).expect("parse failed");
    let plan = planner::plan(&ast, &db.table_schemas).expect("plan failed");
    let pp = plan.pretty_print();

    assert!(pp.contains("Reactive strategy=ReExecute"));
    assert!(pp.contains("invalidation[0] table=users"));
    assert!(pp.contains("INVALIDATE_ON[0] AS inv"));
}
