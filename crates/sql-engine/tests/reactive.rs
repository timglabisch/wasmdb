use std::collections::{HashMap, HashSet};

use sql_engine::execute::{self, Columns, ParamValue};
use sql_engine::planner;
use sql_engine::CallerRegistry;
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
    callers: CallerRegistry,
}

impl TestDb {
    fn new() -> Self {
        Self {
            tables: HashMap::new(),
            table_schemas: HashMap::new(),
            callers: CallerRegistry::new(),
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
        let plan = planner::sql::plan(&ast, &self.table_schemas, &self.callers.requirements).expect("plan failed");
        let mut ctx = execute::ExecutionContext::with_params(&self.tables, params);
        execute::execute_plan(&mut ctx, &plan).expect("execute failed")
    }

    fn run_with_triggered(&self, sql: &str, params: execute::Params, triggered: HashSet<usize>) -> Columns {
        let ast = parser::parse(sql).expect("parse failed");
        let plan = planner::sql::plan(&ast, &self.table_schemas, &self.callers.requirements).expect("plan failed");
        let mut ctx = execute::ExecutionContext::with_params(&self.tables, params);
        ctx.triggered_conditions = Some(triggered);
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

// ── Helpers ──────────────────────────────────────────────────────────────

fn plan_and_subscribe(
    db: &TestDb,
    sql: &str,
    params: &execute::Params,
) -> (SubscriptionRegistry, sql_engine::reactive::SubscriptionId) {
    let ast = parser::parse(sql).expect("parse failed");
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements)
        .expect("plan_reactive failed");
    let mut registry = SubscriptionRegistry::new();
    let sub_id = registry.subscribe(&plan.conditions, &plan.sources, params).unwrap();
    (registry, sub_id)
}

// ── Tests: Basic single-table ───────────────────────────────────────────

#[test]
fn reactive_pk_watch() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv, users.name FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let ast = parser::parse(sql).expect("parse failed");
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).expect("plan_reactive failed");
    assert_eq!(plan.conditions.len(), 1);
    assert_eq!(plan.conditions[0].table, "users");

    let mut registry = SubscriptionRegistry::new();
    let sub_id = registry.subscribe(&plan.conditions, &plan.sources, &params).unwrap();

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
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).expect("plan_reactive failed");

    let mut registry = SubscriptionRegistry::new();
    let sub_id = registry.subscribe(&plan.conditions, &plan.sources, &params).unwrap();

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
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).expect("plan_reactive failed");

    let mut registry = SubscriptionRegistry::new();
    let sub_id = registry.subscribe(&plan.conditions, &plan.sources, &params).unwrap();
    registry.unsubscribe(sub_id);

    assert!(sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(1), CellValue::Str("X".into()), CellValue::I64(1)]).is_empty());
}

#[test]
fn reactive_delete_triggers() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let ast = parser::parse(sql).expect("parse failed");
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).expect("plan_reactive failed");

    let mut registry = SubscriptionRegistry::new();
    let sub_id = registry.subscribe(&plan.conditions, &plan.sources, &params).unwrap();

    assert_eq!(sql_engine::reactive::execute::on_delete(&registry, "users", &[CellValue::I64(1), CellValue::Str("Alice".into()), CellValue::I64(30)]), vec![sub_id]);
}

#[test]
fn reactive_update_leaving_filter() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid AND users.name = 'Alice') AS inv FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let ast = parser::parse(sql).expect("parse failed");
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).expect("plan_reactive failed");

    let mut registry = SubscriptionRegistry::new();
    let sub_id = registry.subscribe(&plan.conditions, &plan.sources, &params).unwrap();

    // UPDATE is represented as delete(old) + insert(new) in a ZSet.
    let mut zset = sql_engine::storage::ZSet::new();
    zset.delete("users".into(), vec![CellValue::I64(1), CellValue::Str("Alice".into()), CellValue::I64(30)]);
    zset.insert("users".into(), vec![CellValue::I64(1), CellValue::Str("Bobby".into()), CellValue::I64(30)]);
    let (affected, trace) = traced_on_zset(&registry, &zset);
    assert!(affected.contains_key(&sub_id));
    // delete row: id=1 + name='Alice' → both lookup keys match (index=2), verify passes
    // insert row: id=1 matches but name='Bobby' ≠ 'Alice' → only one key (index=1), verify fails
    assert_reactive_trace(&trace, "
OnZSet 2 mutations
  DELETE users [1, 'Alice', 30]
    Hash [1, 'Alice'] --> Sub(0)
    Verify 1/1 triggered
      Sub(0) Condition[0] ((users.id = 1 AND users.name = 'Alice')) --> true
  INSERT users [1, 'Bobby', 30]
    Hash [1, 'Bobby'] --> miss
  Condition[0]: run=1/1 total=1/1
");
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
fn reactive_column_in_sql_execution_plan() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv, users.name FROM users WHERE users.id = :uid";
    let ast = parser::parse(sql).expect("parse failed");
    let exec_plan = planner::sql::plan(&ast, &db.table_schemas, &db.callers.requirements).expect("plan failed");
    let pp = exec_plan.pretty_print();
    assert!(pp.contains("REACTIVE[0] AS inv"));
}

#[test]
fn reactive_multi_eq_verify_filter_regression() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid AND users.name = 'Alice') AS inv FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let plan = sql_engine::planner::reactive::plan_reactive(
        &parser::parse(sql).unwrap(),
        &db.table_schemas,
        &db.callers.requirements,
    ).unwrap();

    let mut registry = SubscriptionRegistry::new();
    let sub_id = registry.subscribe(&plan.conditions, &plan.sources, &params).unwrap();

    let affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(1), CellValue::Str("Bob".into()), CellValue::I64(25)]);
    assert!(affected.is_empty(), "should NOT trigger: name mismatch");

    let affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(1), CellValue::Str("Alice".into()), CellValue::I64(30)]);
    assert_eq!(affected, vec![sub_id], "should trigger: full condition matches");

    let affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(99), CellValue::Str("Alice".into()), CellValue::I64(30)]);
    assert!(affected.is_empty(), "should NOT trigger: id mismatch");
}

// ── Tests: Multiple subscriptions ───────────────────────────────────────

#[test]
fn reactive_multiple_subs_same_table() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv FROM users WHERE users.id = :uid";

    let ast = parser::parse(sql).expect("parse failed");
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).unwrap();

    let mut registry = SubscriptionRegistry::new();
    let sub1 = registry.subscribe(&plan.conditions, &plan.sources, &HashMap::from([("uid".into(), ParamValue::Int(1))])).unwrap();
    let sub2 = registry.subscribe(&plan.conditions, &plan.sources, &HashMap::from([("uid".into(), ParamValue::Int(2))])).unwrap();

    // Insert for user 1 → only sub1
    let affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(1), CellValue::Str("X".into()), CellValue::I64(1)]);
    assert_eq!(affected, vec![sub1]);

    // Insert for user 2 → only sub2
    let affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(2), CellValue::Str("Y".into()), CellValue::I64(1)]);
    assert_eq!(affected, vec![sub2]);

    // Insert for user 99 → neither
    let affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(99), CellValue::Str("Z".into()), CellValue::I64(1)]);
    assert!(affected.is_empty());
}

#[test]
fn reactive_multiple_subs_same_key() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv FROM users WHERE users.id = :uid";

    let ast = parser::parse(sql).expect("parse failed");
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).unwrap();

    let mut registry = SubscriptionRegistry::new();
    let sub1 = registry.subscribe(&plan.conditions, &plan.sources, &HashMap::from([("uid".into(), ParamValue::Int(1))])).unwrap();
    let sub2 = registry.subscribe(&plan.conditions, &plan.sources, &HashMap::from([("uid".into(), ParamValue::Int(1))])).unwrap();

    // Both subs watch user 1 → both triggered
    let mut affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(1), CellValue::Str("X".into()), CellValue::I64(1)]);
    affected.sort_by_key(|s| s.0);
    assert_eq!(affected.len(), 2);
    assert!(affected.contains(&sub1));
    assert!(affected.contains(&sub2));
}

// ── Tests: Cross-table isolation ────────────────────────────────────────

#[test]
fn reactive_wrong_table_does_not_trigger() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let (registry, _sub_id) = plan_and_subscribe(&db, sql, &params);

    // Mutation on orders → should NOT trigger users subscription
    let affected = sql_engine::reactive::execute::on_insert(&registry, "orders", &[CellValue::I64(99), CellValue::I64(1), CellValue::I64(500)]);
    assert!(affected.is_empty(), "mutation on different table should not trigger");
}

#[test]
fn reactive_separate_table_subscriptions() {
    let db = make_db();
    let sql_users = "SELECT REACTIVE(users.id = :uid) AS inv FROM users WHERE users.id = :uid";
    let sql_orders = "SELECT REACTIVE(orders.user_id = :uid) AS inv FROM orders WHERE orders.user_id = :uid";

    let ast_u = parser::parse(sql_users).unwrap();
    let ast_o = parser::parse(sql_orders).unwrap();
    let plan_u = sql_engine::planner::reactive::plan_reactive(&ast_u, &db.table_schemas, &db.callers.requirements).unwrap();
    let plan_o = sql_engine::planner::reactive::plan_reactive(&ast_o, &db.table_schemas, &db.callers.requirements).unwrap();

    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);
    let mut registry = SubscriptionRegistry::new();
    let sub_u = registry.subscribe(&plan_u.conditions, &plan_u.sources, &params).unwrap();
    let sub_o = registry.subscribe(&plan_o.conditions, &plan_o.sources, &params).unwrap();

    // Mutation on users → only user sub
    let affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(1), CellValue::Str("X".into()), CellValue::I64(1)]);
    assert_eq!(affected, vec![sub_u]);

    // Mutation on orders → only order sub
    let affected = sql_engine::reactive::execute::on_insert(&registry, "orders", &[CellValue::I64(99), CellValue::I64(1), CellValue::I64(999)]);
    assert_eq!(affected, vec![sub_o]);
}

// ── Tests: Range conditions ─────────────────────────────────────────────

#[test]
fn reactive_range_gt() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.age > 30) AS inv FROM users";

    let (registry, sub_id) = plan_and_subscribe(&db, sql, &HashMap::new());

    // age=35 → triggers (> 30)
    let affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(10), CellValue::Str("X".into()), CellValue::I64(35)]);
    assert_eq!(affected, vec![sub_id]);

    // age=30 → does NOT trigger (not > 30)
    let affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(11), CellValue::Str("Y".into()), CellValue::I64(30)]);
    assert!(affected.is_empty());

    // age=25 → does NOT trigger
    let affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(12), CellValue::Str("Z".into()), CellValue::I64(25)]);
    assert!(affected.is_empty());
}

#[test]
fn reactive_range_lt() {
    let db = make_db();
    let sql = "SELECT REACTIVE(orders.amount < 100) AS inv FROM orders";

    let (registry, sub_id) = plan_and_subscribe(&db, sql, &HashMap::new());

    // amount=50 → triggers
    let affected = sql_engine::reactive::execute::on_insert(&registry, "orders", &[CellValue::I64(10), CellValue::I64(1), CellValue::I64(50)]);
    assert_eq!(affected, vec![sub_id]);

    // amount=100 → does not trigger
    let affected = sql_engine::reactive::execute::on_insert(&registry, "orders", &[CellValue::I64(11), CellValue::I64(1), CellValue::I64(100)]);
    assert!(affected.is_empty());
}

#[test]
fn reactive_eq_and_range_combined() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid AND users.age > 25) AS inv FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let (registry, sub_id) = plan_and_subscribe(&db, sql, &params);

    // id=1, age=30 → triggers (both match)
    let affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(1), CellValue::Str("X".into()), CellValue::I64(30)]);
    assert_eq!(affected, vec![sub_id]);

    // id=1, age=20 → does NOT trigger (age too low)
    let affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(1), CellValue::Str("Y".into()), CellValue::I64(20)]);
    assert!(affected.is_empty());

    // id=99, age=30 → does NOT trigger (wrong id, not even a candidate)
    let affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(99), CellValue::Str("Z".into()), CellValue::I64(30)]);
    assert!(affected.is_empty());
}

// ── Tests: UPDATE (ZSet delete+insert) ──────────────────────────────────

#[test]
fn reactive_update_staying_in_filter() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let (registry, sub_id) = plan_and_subscribe(&db, sql, &params);

    // UPDATE users SET name='Bob' WHERE id=1 → ZSet: delete old + insert new
    let mut zset = sql_engine::storage::ZSet::new();
    zset.delete("users".into(), vec![CellValue::I64(1), CellValue::Str("Alice".into()), CellValue::I64(30)]);
    zset.insert("users".into(), vec![CellValue::I64(1), CellValue::Str("Bob".into()), CellValue::I64(30)]);

    let (affected, trace) = traced_on_zset(&registry, &zset);
    assert!(affected.contains_key(&sub_id), "update staying in filter should trigger");
    assert_reactive_trace(&trace, "
OnZSet 2 mutations
  DELETE users [1, 'Alice', 30]
    Hash [1] --> Sub(0)
    Verify 1/1 triggered
      Sub(0) Condition[0] (users.id = 1) --> true
  INSERT users [1, 'Bob', 30]
    Hash [1] --> Sub(0)
    Verify 1/1 triggered
      Sub(0) Condition[0] (users.id = 1) --> true
  Condition[0]: run=2/2 total=2/2
");
}

#[test]
fn reactive_update_entering_filter() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid AND users.name = 'Alice') AS inv FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let (registry, sub_id) = plan_and_subscribe(&db, sql, &params);

    // UPDATE: name changes from "Other" to "Alice" — entering the filter
    let mut zset = sql_engine::storage::ZSet::new();
    zset.delete("users".into(), vec![CellValue::I64(1), CellValue::Str("Other".into()), CellValue::I64(30)]);
    zset.insert("users".into(), vec![CellValue::I64(1), CellValue::Str("Alice".into()), CellValue::I64(30)]);

    let (affected, trace) = traced_on_zset(&registry, &zset);
    assert!(affected.contains_key(&sub_id), "update entering filter should trigger");
    // delete row: id=1 matches index, but name='Other' fails verify → triggered=0
    // insert row: id=1 + name='Alice' both match index, verify passes → triggered=1
    assert_reactive_trace(&trace, "
OnZSet 2 mutations
  DELETE users [1, 'Other', 30]
    Hash [1, 'Other'] --> miss
  INSERT users [1, 'Alice', 30]
    Hash [1, 'Alice'] --> Sub(0)
    Verify 1/1 triggered
      Sub(0) Condition[0] ((users.id = 1 AND users.name = 'Alice')) --> true
  Condition[0]: run=1/1 total=1/1
");
}

#[test]
fn reactive_update_no_match_neither_old_nor_new() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let (registry, _sub_id) = plan_and_subscribe(&db, sql, &params);

    // UPDATE on user 99 — neither old nor new matches id=1
    let mut zset = sql_engine::storage::ZSet::new();
    zset.delete("users".into(), vec![CellValue::I64(99), CellValue::Str("X".into()), CellValue::I64(1)]);
    zset.insert("users".into(), vec![CellValue::I64(99), CellValue::Str("Y".into()), CellValue::I64(1)]);

    let (affected, trace) = traced_on_zset(&registry, &zset);
    assert!(affected.is_empty(), "update on unrelated row should not trigger");
    assert_reactive_trace(&trace, "
OnZSet 2 mutations
  DELETE users [99, 'X', 1]
    Hash [99] --> miss
  INSERT users [99, 'Y', 1]
    Hash [99] --> miss
");
}

// ── Tests: ZSet with multiple entries ───────────────────────────────────

#[test]
fn reactive_zset_multiple_entries() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv FROM users WHERE users.id = :uid";

    let ast = parser::parse(sql).unwrap();
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).unwrap();

    let mut registry = SubscriptionRegistry::new();
    let sub1 = registry.subscribe(&plan.conditions, &plan.sources, &HashMap::from([("uid".into(), ParamValue::Int(1))])).unwrap();
    let sub2 = registry.subscribe(&plan.conditions, &plan.sources, &HashMap::from([("uid".into(), ParamValue::Int(2))])).unwrap();

    // ZSet with inserts for both user 1 and user 2
    let mut zset = sql_engine::storage::ZSet::new();
    zset.insert("users".into(), vec![CellValue::I64(1), CellValue::Str("A".into()), CellValue::I64(1)]);
    zset.insert("users".into(), vec![CellValue::I64(2), CellValue::Str("B".into()), CellValue::I64(2)]);
    zset.insert("users".into(), vec![CellValue::I64(99), CellValue::Str("C".into()), CellValue::I64(3)]);

    let (affected, trace) = traced_on_zset(&registry, &zset);
    assert!(affected.contains_key(&sub1), "sub1 should be triggered by user 1 insert");
    assert!(affected.contains_key(&sub2), "sub2 should be triggered by user 2 insert");
    assert_eq!(affected.len(), 2, "only 2 subs should be triggered, not the user 99 row");
    assert_reactive_trace(&trace, "
OnZSet 3 mutations
  INSERT users [1, 'A', 1]
    Hash [1] --> Sub(0)
    Verify 1/1 triggered
      Sub(0) Condition[0] (users.id = 1) --> true
  INSERT users [2, 'B', 2]
    Hash [2] --> Sub(1)
    Verify 1/1 triggered
      Sub(1) Condition[0] (users.id = 2) --> true
  INSERT users [99, 'C', 3]
    Hash [99] --> miss
  Condition[0]: run=2/2 total=2/2
");
}

#[test]
fn reactive_zset_mixed_tables() {
    let db = make_db();
    let sql_users = "SELECT REACTIVE(users.id = :uid) AS inv FROM users WHERE users.id = :uid";
    let sql_orders = "SELECT REACTIVE(orders.user_id = :uid) AS inv FROM orders WHERE orders.user_id = :uid";

    let ast_u = parser::parse(sql_users).unwrap();
    let ast_o = parser::parse(sql_orders).unwrap();
    let plan_u = sql_engine::planner::reactive::plan_reactive(&ast_u, &db.table_schemas, &db.callers.requirements).unwrap();
    let plan_o = sql_engine::planner::reactive::plan_reactive(&ast_o, &db.table_schemas, &db.callers.requirements).unwrap();

    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);
    let mut registry = SubscriptionRegistry::new();
    let sub_u = registry.subscribe(&plan_u.conditions, &plan_u.sources, &params).unwrap();
    let sub_o = registry.subscribe(&plan_o.conditions, &plan_o.sources, &params).unwrap();

    // ZSet with entries from both tables
    let mut zset = sql_engine::storage::ZSet::new();
    zset.insert("users".into(), vec![CellValue::I64(1), CellValue::Str("New".into()), CellValue::I64(1)]);
    zset.insert("orders".into(), vec![CellValue::I64(99), CellValue::I64(1), CellValue::I64(999)]);

    let (affected, trace) = traced_on_zset(&registry, &zset);
    assert!(affected.contains_key(&sub_u));
    assert!(affected.contains_key(&sub_o));
    assert_reactive_trace(&trace, "
OnZSet 2 mutations
  INSERT users [1, 'New', 1]
    Hash [1] --> Sub(0)
    Verify 1/1 triggered
      Sub(0) Condition[0] (users.id = 1) --> true
  INSERT orders [99, 1, 999]
    Hash [1] --> Sub(1)
    Verify 1/1 triggered
      Sub(1) Condition[0] (orders.user_id = 1) --> true
  Condition[0]: run=2/2 total=2/2
");
}

// ── Tests: Table-level subscriptions (no predicate) ─────────────────────

#[test]
fn reactive_table_level_triggers_on_any_mutation() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id) AS inv, users.name FROM users";

    let (registry, sub_id) = plan_and_subscribe(&db, sql, &HashMap::new());

    // Any insert on users → triggers
    let affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(99), CellValue::Str("Anyone".into()), CellValue::I64(1)]);
    assert_eq!(affected, vec![sub_id]);

    // Any delete on users → triggers
    let affected = sql_engine::reactive::execute::on_delete(&registry, "users", &[CellValue::I64(1), CellValue::Str("Alice".into()), CellValue::I64(30)]);
    assert_eq!(affected, vec![sub_id]);

    // Mutation on orders → does NOT trigger
    let affected = sql_engine::reactive::execute::on_insert(&registry, "orders", &[CellValue::I64(1), CellValue::I64(1), CellValue::I64(100)]);
    assert!(affected.is_empty());
}

// ── Tests: Plan inspection ──────────────────────────────────────────────

#[test]
fn reactive_plan_condition_count() {
    let db = make_db();

    // Single REACTIVE → 1 condition
    let ast = parser::parse("SELECT REACTIVE(users.id = :uid) AS inv FROM users WHERE users.id = :uid").unwrap();
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).unwrap();
    assert_eq!(plan.conditions.len(), 1);
    assert_eq!(plan.conditions[0].table, "users");

    // Table-level REACTIVE → 1 condition
    let ast = parser::parse("SELECT REACTIVE(users.id) AS inv FROM users").unwrap();
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).unwrap();
    assert_eq!(plan.conditions.len(), 1);
    assert_eq!(plan.conditions[0].table, "users");
}

#[test]
fn reactive_plan_range_condition_is_table_scan() {
    let db = make_db();
    // Range-only → no equality key → TableScan strategy
    let ast = parser::parse("SELECT REACTIVE(users.age > 30) AS inv FROM users").unwrap();
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).unwrap();
    assert_eq!(plan.conditions.len(), 1);
    assert!(matches!(
        plan.conditions[0].strategy,
        sql_engine::planner::reactive::ReactiveLookupStrategy::TableScan
    ));
}

#[test]
fn reactive_plan_eq_condition_is_index_lookup() {
    let db = make_db();
    let ast = parser::parse("SELECT REACTIVE(users.id = :uid) AS inv FROM users WHERE users.id = :uid").unwrap();
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).unwrap();
    assert_eq!(plan.conditions.len(), 1);
    match &plan.conditions[0].strategy {
        sql_engine::planner::reactive::ReactiveLookupStrategy::IndexLookup { lookup_key_sets } => {
            assert_eq!(lookup_key_sets.len(), 1);
            assert_eq!(lookup_key_sets[0].len(), 1);
            assert_eq!(lookup_key_sets[0][0].col, 0); // users.id is column 0
        }
        _ => panic!("expected IndexLookup for equality condition"),
    }
}

#[test]
fn reactive_plan_mixed_eq_range_extracts_eq_key() {
    let db = make_db();
    let ast = parser::parse("SELECT REACTIVE(orders.user_id = :uid AND orders.amount > 100) AS inv FROM orders WHERE orders.user_id = :uid").unwrap();
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).unwrap();
    assert_eq!(plan.conditions.len(), 1);
    match &plan.conditions[0].strategy {
        sql_engine::planner::reactive::ReactiveLookupStrategy::IndexLookup { lookup_key_sets } => {
            assert_eq!(lookup_key_sets.len(), 1);
            assert_eq!(lookup_key_sets[0].len(), 1);
            assert_eq!(lookup_key_sets[0][0].col, 1); // orders.user_id is column 1
        }
        _ => panic!("expected IndexLookup: equality should be extracted even with range"),
    }
}

// ── Tests: Unsubscribe edge cases ───────────────────────────────────────

#[test]
fn reactive_unsubscribe_one_of_many() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv FROM users WHERE users.id = :uid";

    let ast = parser::parse(sql).unwrap();
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).unwrap();

    let mut registry = SubscriptionRegistry::new();
    let sub1 = registry.subscribe(&plan.conditions, &plan.sources, &HashMap::from([("uid".into(), ParamValue::Int(1))])).unwrap();
    let sub2 = registry.subscribe(&plan.conditions, &plan.sources, &HashMap::from([("uid".into(), ParamValue::Int(1))])).unwrap();

    // Unsubscribe sub1, sub2 should still work
    registry.unsubscribe(sub1);

    let affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(1), CellValue::Str("X".into()), CellValue::I64(1)]);
    assert_eq!(affected, vec![sub2]);
}

#[test]
fn reactive_unsubscribe_nonexistent_is_noop() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let (mut registry, sub_id) = plan_and_subscribe(&db, sql, &params);

    // Unsubscribe a non-existent ID — should not panic
    registry.unsubscribe(sql_engine::reactive::SubscriptionId(999));

    // Original sub still works
    let affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(1), CellValue::Str("X".into()), CellValue::I64(1)]);
    assert_eq!(affected, vec![sub_id]);
}

// ── Tests: Query execution with reactive columns ────────────────────────

#[test]
fn reactive_select_returns_zero_without_triggered() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv, users.name, users.age FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let result = db.run_with_params(sql, params);
    // 3 columns: inv, name, age
    assert_eq!(result.len(), 3);
    // inv = 0 (no triggered conditions)
    assert_eq!(result[0], vec![CellValue::I64(0)]);
    // name = Alice
    assert_eq!(result[1], vec![CellValue::Str("Alice".into())]);
    // age = 30
    assert_eq!(result[2], vec![CellValue::I64(30)]);
}

#[test]
fn reactive_select_multiple_rows() {
    let db = make_db();
    // No WHERE filter → returns all users, reactive column is 0 for each
    let sql = "SELECT REACTIVE(users.age > 30) AS inv, users.name FROM users";
    let params: execute::Params = HashMap::new();

    let result = db.run_with_params(sql, params);
    assert_eq!(result.len(), 2); // inv, name
    // 3 users in the table
    assert_eq!(result[0].len(), 3);
    // All reactive columns are 0 (no triggered conditions in fresh query)
    assert!(result[0].iter().all(|v| *v == CellValue::I64(0)));
}

// ── Tests: JOIN + reactive ──────────────────────────────────────────────

#[test]
fn reactive_with_join_monitors_single_table() {
    let db = make_db();
    // REACTIVE on orders.user_id in a query that joins users and orders
    let sql = "SELECT REACTIVE(orders.user_id = :uid) AS inv, users.name, orders.amount \
               FROM users \
               INNER JOIN orders ON users.id = orders.user_id \
               WHERE orders.user_id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    // Plan should work — reactive condition targets orders table
    let ast = parser::parse(sql).expect("parse failed");
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).unwrap();
    assert_eq!(plan.conditions.len(), 1);
    assert_eq!(plan.conditions[0].table, "orders");

    let mut registry = SubscriptionRegistry::new();
    let sub_id = registry.subscribe(&plan.conditions, &plan.sources, &params).unwrap();

    // Insert on orders for user_id=1 → triggers
    let affected = sql_engine::reactive::execute::on_insert(&registry, "orders", &[CellValue::I64(99), CellValue::I64(1), CellValue::I64(500)]);
    assert_eq!(affected, vec![sub_id]);

    // Insert on users → does NOT trigger (reactive watches orders)
    let affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(1), CellValue::Str("X".into()), CellValue::I64(1)]);
    assert!(affected.is_empty());
}

#[test]
fn reactive_join_query_executes() {
    let db = make_db();
    let sql = "SELECT REACTIVE(orders.user_id = :uid) AS inv, users.name, orders.amount \
               FROM users \
               INNER JOIN orders ON users.id = orders.user_id \
               WHERE orders.user_id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let result = db.run_with_params(sql, params);
    // Should return: inv, name, amount — Alice has 2 orders (100, 200)
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].len(), 2); // 2 rows (Alice's orders)
    assert_eq!(result[1], vec![CellValue::Str("Alice".into()), CellValue::Str("Alice".into())]);
}

// ── Reactive plan snapshots ──────────────────────────────────────────────

fn reactive_plan(db: &TestDb, sql: &str) -> String {
    let ast = parser::parse(sql).expect("parse failed");
    sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements)
        .expect("plan_reactive failed")
        .pretty_print()
}

fn assert_reactive_plan(actual: &str, expected: &str) {
    let actual = actual.trim_end();
    let expected = expected.trim();
    assert_eq!(actual, expected, "\n\n--- ACTUAL ---\n{actual}\n\n--- EXPECTED ---\n{expected}\n");
}

#[test]
fn reactive_plan_snapshot_pk_eq() {
    let db = make_db();
    assert_reactive_plan(&reactive_plan(&db,
        "SELECT REACTIVE(users.id = :uid) AS inv FROM users WHERE users.id = :uid"), "
Reactive[0] table=users strategy=IndexLookup [users.id = :uid]
  verify: users.id = :uid
");
}

#[test]
fn reactive_plan_snapshot_table_level() {
    let db = make_db();
    assert_reactive_plan(&reactive_plan(&db,
        "SELECT REACTIVE(users.id) AS inv FROM users"), "
Reactive[0] table=users strategy=TableScan
");
}

#[test]
fn reactive_plan_snapshot_range_only() {
    let db = make_db();
    assert_reactive_plan(&reactive_plan(&db,
        "SELECT REACTIVE(users.age > 30) AS inv FROM users"), "
Reactive[0] table=users strategy=TableScan
  verify: users.age > 30
");
}

#[test]
fn reactive_plan_snapshot_eq_and_range() {
    let db = make_db();
    assert_reactive_plan(&reactive_plan(&db,
        "SELECT REACTIVE(users.id = :uid AND users.age > 25) AS inv FROM users WHERE users.id = :uid"), "
Reactive[0] table=users strategy=IndexLookup [users.id = :uid]
  verify: (users.id = :uid AND users.age > 25)
");
}

#[test]
fn reactive_plan_snapshot_multi_eq() {
    let db = make_db();
    assert_reactive_plan(&reactive_plan(&db,
        "SELECT REACTIVE(users.id = :uid AND users.name = 'Alice') AS inv FROM users WHERE users.id = :uid"), "
Reactive[0] table=users strategy=IndexLookup [users.id = :uid, users.name = 'Alice']
  verify: (users.id = :uid AND users.name = 'Alice')
");
}

#[test]
fn reactive_plan_snapshot_two_conditions_same_table() {
    let db = make_db();
    assert_reactive_plan(&reactive_plan(&db,
        "SELECT REACTIVE(users.id = :uid) AS inv_id, REACTIVE(users.age > 30) AS inv_age FROM users WHERE users.id = :uid"), "
Reactive[0] table=users strategy=IndexLookup [users.id = :uid]
  verify: users.id = :uid
Reactive[1] table=users strategy=TableScan
  verify: users.age > 30
");
}

#[test]
fn reactive_plan_snapshot_two_conditions_different_tables() {
    let db = make_db();
    assert_reactive_plan(&reactive_plan(&db,
        "SELECT REACTIVE(users.id = :uid) AS inv_user, REACTIVE(orders.user_id = :uid) AS inv_order \
         FROM users INNER JOIN orders ON users.id = orders.user_id WHERE users.id = :uid"), "
Reactive[0] table=users strategy=IndexLookup [users.id = :uid]
  verify: users.id = :uid
Reactive[1] table=orders strategy=IndexLookup [orders.user_id = :uid]
  verify: orders.user_id = :uid
");
}

// ── Tests: Multiple REACTIVE() expressions ──────────────────────────────

#[test]
fn reactive_two_conditions_same_table() {
    let db = make_db();
    // Two REACTIVE() columns on the same table, different predicates
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv_id, REACTIVE(users.age > 30) AS inv_age, users.name FROM users WHERE users.id = :uid";
    let ast = parser::parse(sql).expect("parse failed");
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).unwrap();
    assert_eq!(plan.conditions.len(), 2, "should extract 2 reactive conditions");
    assert_eq!(plan.conditions[0].table, "users");
    assert_eq!(plan.conditions[1].table, "users");

    // Plan should have REACTIVE[0] and REACTIVE[1]
    let plan = planner::sql::plan(&ast, &db.table_schemas, &db.callers.requirements).expect("plan failed");
    let pp = plan.pretty_print();
    assert!(pp.contains("REACTIVE[0] AS inv_id"), "pp: {pp}");
    assert!(pp.contains("REACTIVE[1] AS inv_age"), "pp: {pp}");
}

#[test]
fn reactive_two_conditions_different_tables() {
    let db = make_db();
    // Two REACTIVE() columns on different tables in a JOIN query
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv_user, REACTIVE(orders.user_id = :uid) AS inv_order, users.name \
               FROM users \
               INNER JOIN orders ON users.id = orders.user_id \
               WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let ast = parser::parse(sql).expect("parse failed");
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).unwrap();
    assert_eq!(plan.conditions.len(), 2);
    assert_eq!(plan.conditions[0].table, "users");
    assert_eq!(plan.conditions[1].table, "orders");

    let mut registry = SubscriptionRegistry::new();
    let sub_id = registry.subscribe(&plan.conditions, &plan.sources, &params).unwrap();

    // Insert on users for id=1 → triggers (condition 0)
    let affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(1), CellValue::Str("X".into()), CellValue::I64(1)]);
    assert_eq!(affected, vec![sub_id]);

    // Insert on orders for user_id=1 → triggers (condition 1)
    let affected = sql_engine::reactive::execute::on_insert(&registry, "orders", &[CellValue::I64(99), CellValue::I64(1), CellValue::I64(500)]);
    assert_eq!(affected, vec![sub_id]);

    // Insert on orders for user_id=99 → does NOT trigger
    let affected = sql_engine::reactive::execute::on_insert(&registry, "orders", &[CellValue::I64(100), CellValue::I64(99), CellValue::I64(500)]);
    assert!(affected.is_empty());
}

#[test]
fn reactive_two_conditions_triggered_indices() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv_id, REACTIVE(users.age > 30) AS inv_age FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let ast = parser::parse(sql).expect("parse failed");
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).unwrap();

    let mut registry = SubscriptionRegistry::new();
    let sub_id = registry.subscribe(&plan.conditions, &plan.sources, &params).unwrap();

    // Insert id=1, age=35 → both conditions match → indices {0, 1}
    let mut zset1 = sql_engine::storage::ZSet::new();
    zset1.insert("users".into(), vec![CellValue::I64(1), CellValue::Str("X".into()), CellValue::I64(35)]);
    let (affected, trace) = traced_on_zset(&registry, &zset1);
    assert!(affected.contains_key(&sub_id));
    let indices = &affected[&sub_id];
    assert!(indices.contains(&0), "condition 0 (id=1) should trigger");
    assert!(indices.contains(&1), "condition 1 (age>30) should trigger");
    // condition 0 is IndexLookup, condition 1 is TableScan → both index and table_level hits
    assert_reactive_trace(&trace, "
OnZSet 1 mutations
  INSERT users [1, 'X', 35]
    Hash [1] --> Sub(0)
    Scan --> Sub(0)
    Verify 1/1 triggered
      Sub(0) Condition[0] (users.id = 1) --> true
      Sub(0) Condition[1] (users.age > 30) --> true
  Condition[0]: run=1/1 total=1/1
  Condition[1]: run=1/1 total=1/1
");

    // Insert id=1, age=25 → only condition 0 matches (id=1, but age not >30)
    let mut zset2 = sql_engine::storage::ZSet::new();
    zset2.insert("users".into(), vec![CellValue::I64(1), CellValue::Str("Y".into()), CellValue::I64(25)]);
    let (affected, trace) = traced_on_zset(&registry, &zset2);
    assert!(affected.contains_key(&sub_id));
    let indices = &affected[&sub_id];
    assert!(indices.contains(&0), "condition 0 (id=1) should trigger");
    assert!(!indices.contains(&1), "condition 1 (age>30) should NOT trigger");
    // Same candidates, condition 0 passes but condition 1 fails
    assert_reactive_trace(&trace, "
OnZSet 1 mutations
  INSERT users [1, 'Y', 25]
    Hash [1] --> Sub(0)
    Scan --> Sub(0)
    Verify 1/1 triggered
      Sub(0) Condition[0] (users.id = 1) --> true
      Sub(0) Condition[1] (users.age > 30) --> false
  Condition[0]: run=1/1 total=1/1
  Condition[1]: run=1/0 total=1/0
");
}

#[test]
fn reactive_two_conditions_execute_returns_both_columns() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv_id, REACTIVE(users.age > 30) AS inv_age, users.name FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let result = db.run_with_params(sql, params);
    // 3 columns: inv_id, inv_age, name
    assert_eq!(result.len(), 3);
    // Both reactive columns are 0 (no triggered conditions)
    assert_eq!(result[0], vec![CellValue::I64(0)]);
    assert_eq!(result[1], vec![CellValue::I64(0)]);
    assert_eq!(result[2], vec![CellValue::Str("Alice".into())]);
}

// ── Tests: Materialized subqueries + reactive ───────────────────────────

#[test]
fn reactive_with_in_subquery_plans() {
    let db = make_db();
    // Reactive condition + WHERE with IN subquery
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv, users.name \
               FROM users \
               WHERE users.id IN (SELECT orders.user_id FROM orders WHERE orders.amount > 100)";

    let ast = parser::parse(sql).expect("parse failed");

    // Plan should work — materialization step + reactive condition
    let plan = planner::sql::plan(&ast, &db.table_schemas, &db.callers.requirements).expect("plan failed");
    assert!(!plan.materializations.is_empty(), "should have materialization step");

    let pp = plan.pretty_print();
    assert!(pp.contains("REACTIVE[0] AS inv"));
    assert!(pp.contains("Materialize"));

    // Reactive conditions should be extractable
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).unwrap();
    assert_eq!(plan.conditions.len(), 1);
    assert_eq!(plan.conditions[0].table, "users");
}

#[test]
fn reactive_with_in_subquery_executes() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv, users.name \
               FROM users \
               WHERE users.id IN (SELECT orders.user_id FROM orders WHERE orders.amount > 100)";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let result = db.run_with_params(sql, params);
    // The subquery selects user_ids where amount > 100: user 1 (has order 200)
    // So only Alice should be in the result
    assert_eq!(result.len(), 2); // inv, name
    assert!(result[1].contains(&CellValue::Str("Alice".into())));
}

#[test]
fn reactive_with_in_subquery_subscription() {
    let db = make_db();
    // The reactive condition is on users.id — independent of the subquery filter
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv, users.name \
               FROM users \
               WHERE users.id IN (SELECT orders.user_id FROM orders WHERE orders.amount > 100)";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let ast = parser::parse(sql).expect("parse failed");
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).unwrap();

    let mut registry = SubscriptionRegistry::new();
    let sub_id = registry.subscribe(&plan.conditions, &plan.sources, &params).unwrap();

    // Insert on users with id=1 → triggers (reactive watches users.id = :uid)
    let affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(1), CellValue::Str("X".into()), CellValue::I64(1)]);
    assert_eq!(affected, vec![sub_id]);

    // Insert on users with id=99 → does NOT trigger
    let affected = sql_engine::reactive::execute::on_insert(&registry, "users", &[CellValue::I64(99), CellValue::Str("Y".into()), CellValue::I64(1)]);
    assert!(affected.is_empty());
}

#[test]
fn reactive_with_scalar_subquery_plans() {
    let db = make_db();
    // Reactive + scalar subquery comparison
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv, users.name \
               FROM users \
               WHERE users.age > (SELECT orders.amount FROM orders WHERE orders.id = 1)";

    let ast = parser::parse(sql).expect("parse failed");
    let plan = planner::sql::plan(&ast, &db.table_schemas, &db.callers.requirements).expect("plan failed");
    assert!(!plan.materializations.is_empty(), "should have scalar materialization");

    let pp = plan.pretty_print();
    assert!(pp.contains("REACTIVE[0] AS inv"));
    assert!(pp.contains("Materialize"));
}

#[test]
fn reactive_rejects_in_subquery_inside_argument() {
    // Subqueries in the WHERE clause are fine (tests above), but a subquery
    // *inside* REACTIVE(...) cannot be tracked by the current point-lookup
    // reactive model — see planner::reactive::extract for the full reasoning.
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id IN (SELECT orders.user_id FROM orders)) AS inv \
               FROM users";

    let ast = parser::parse(sql).expect("parse failed");
    let err = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements)
        .expect_err("expected plan error for subquery inside REACTIVE()");
    let msg = format!("{err}");
    assert!(
        msg.contains("subqueries are not supported"),
        "unexpected error message: {msg}"
    );
}

#[test]
fn reactive_rejects_scalar_subquery_inside_argument() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.age > (SELECT orders.amount FROM orders WHERE orders.id = 1)) AS inv \
               FROM users";

    let ast = parser::parse(sql).expect("parse failed");
    let err = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements)
        .expect_err("expected plan error for scalar subquery inside REACTIVE()");
    assert!(format!("{err}").contains("subqueries are not supported"));
}

// ── Tests: LEFT JOIN + reactive ─────────────────────────────────────────

#[test]
fn reactive_with_left_join() {
    let db = make_db();
    let sql = "SELECT REACTIVE(orders.user_id = :uid) AS inv, users.name, orders.amount \
               FROM users \
               LEFT JOIN orders ON users.id = orders.user_id \
               WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    // Plan should work with LEFT JOIN
    let ast = parser::parse(sql).expect("parse failed");
    let plan = planner::sql::plan(&ast, &db.table_schemas, &db.callers.requirements).expect("plan failed");
    let pp = plan.pretty_print();
    assert!(pp.contains("Join type=Left"));

    // Reactive condition targets orders
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).unwrap();
    assert_eq!(plan.conditions[0].table, "orders");

    let mut registry = SubscriptionRegistry::new();
    let sub_id = registry.subscribe(&plan.conditions, &plan.sources, &params).unwrap();

    // Insert on orders for user_id=1 → triggers
    let affected = sql_engine::reactive::execute::on_insert(&registry, "orders", &[CellValue::I64(99), CellValue::I64(1), CellValue::I64(500)]);
    assert_eq!(affected, vec![sub_id]);
}

#[test]
fn reactive_left_join_query_executes() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv, users.name, orders.amount \
               FROM users \
               LEFT JOIN orders ON users.id = orders.user_id \
               WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    let result = db.run_with_params(sql, params);
    // Alice has 2 orders (100, 200)
    assert_eq!(result.len(), 3); // inv, name, amount
    assert_eq!(result[0].len(), 2); // 2 rows
}

// ── Tests: ORDER BY / LIMIT with reactive ───────────────────────────────

#[test]
fn reactive_with_order_by() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.age > 20) AS inv, users.name, users.age FROM users ORDER BY users.age DESC";
    let result = db.run_with_params(sql, HashMap::new());

    assert_eq!(result.len(), 3); // inv, name, age
    assert_eq!(result[0].len(), 3); // 3 users
    // Ordered by age DESC: Carol(35), Alice(30), Bob(25)
    assert_eq!(result[2], vec![CellValue::I64(35), CellValue::I64(30), CellValue::I64(25)]);
}

#[test]
fn reactive_with_limit() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.age > 20) AS inv, users.name FROM users LIMIT 2";
    let result = db.run_with_params(sql, HashMap::new());

    assert_eq!(result.len(), 2); // inv, name
    assert_eq!(result[0].len(), 2); // limited to 2 rows
}

// ── Tests: End-to-end (subscribe → mutation → query with triggered) ─────

#[test]
fn reactive_e2e_single_condition_triggered() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv, users.name FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    // 1. Plan + subscribe
    let ast = parser::parse(sql).expect("parse failed");
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).unwrap();
    let mut registry = SubscriptionRegistry::new();
    let sub_id = registry.subscribe(&plan.conditions, &plan.sources, &params).unwrap();

    // 2. Simulate mutation → on_zset → get triggered indices
    let mut zset = sql_engine::storage::ZSet::new();
    zset.insert("users".into(), vec![CellValue::I64(1), CellValue::Str("NewAlice".into()), CellValue::I64(31)]);
    let (affected, trace) = traced_on_zset(&registry, &zset);
    let triggered = affected.get(&sub_id).expect("sub should be triggered");
    assert!(triggered.contains(&0));
    assert_reactive_trace(&trace, "
OnZSet 1 mutations
  INSERT users [1, 'NewAlice', 31]
    Hash [1] --> Sub(0)
    Verify 1/1 triggered
      Sub(0) Condition[0] (users.id = 1) --> true
  Condition[0]: run=1/1 total=1/1
");

    // 3. Re-query with triggered conditions → REACTIVE column should be 1
    let triggered_std: HashSet<usize> = triggered.iter().copied().collect();
    let result = db.run_with_triggered(sql, params.clone(), triggered_std);
    assert_eq!(result[0], vec![CellValue::I64(1)], "REACTIVE column should be 1 when triggered");
    assert_eq!(result[1], vec![CellValue::Str("Alice".into())]);
}

#[test]
fn reactive_e2e_single_condition_not_triggered() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv, users.name FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    // 1. Plan + subscribe
    let ast = parser::parse(sql).expect("parse failed");
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).unwrap();
    let mut registry = SubscriptionRegistry::new();
    let _sub_id = registry.subscribe(&plan.conditions, &plan.sources, &params).unwrap();

    // 2. Mutation on unrelated row → not triggered
    let mut zset = sql_engine::storage::ZSet::new();
    zset.insert("users".into(), vec![CellValue::I64(99), CellValue::Str("Nobody".into()), CellValue::I64(20)]);
    let (affected, trace) = traced_on_zset(&registry, &zset);
    assert!(affected.is_empty());
    assert_reactive_trace(&trace, "
OnZSet 1 mutations
  INSERT users [99, 'Nobody', 20]
    Hash [99] --> miss
");

    // 3. Query without triggered conditions → REACTIVE column should be 0
    let result = db.run_with_params(sql, params);
    assert_eq!(result[0], vec![CellValue::I64(0)], "REACTIVE column should be 0 when not triggered");
}

#[test]
fn reactive_e2e_two_conditions_partial_trigger() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv_id, REACTIVE(users.age > 30) AS inv_age, users.name FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    // 1. Plan + subscribe
    let ast = parser::parse(sql).expect("parse failed");
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).unwrap();
    let mut registry = SubscriptionRegistry::new();
    let sub_id = registry.subscribe(&plan.conditions, &plan.sources, &params).unwrap();

    // 2. Mutation: id=1, age=25 → condition 0 (id match) triggers, condition 1 (age>30) does NOT
    let mut zset = sql_engine::storage::ZSet::new();
    zset.insert("users".into(), vec![CellValue::I64(1), CellValue::Str("X".into()), CellValue::I64(25)]);
    let (affected, trace) = traced_on_zset(&registry, &zset);
    let triggered = affected.get(&sub_id).unwrap();
    assert!(triggered.contains(&0), "condition 0 should trigger");
    assert!(!triggered.contains(&1), "condition 1 should NOT trigger");
    // condition 0 is IndexLookup (id=1), condition 1 is TableScan (age>30)
    // sub found via both paths, but only condition 0 passes verify
    assert_reactive_trace(&trace, "
OnZSet 1 mutations
  INSERT users [1, 'X', 25]
    Hash [1] --> Sub(0)
    Scan --> Sub(0)
    Verify 1/1 triggered
      Sub(0) Condition[0] (users.id = 1) --> true
      Sub(0) Condition[1] (users.age > 30) --> false
  Condition[0]: run=1/1 total=1/1
  Condition[1]: run=1/0 total=1/0
");

    // 3. Re-query with triggered {0} → inv_id=1, inv_age=0
    let triggered_std: HashSet<usize> = triggered.iter().copied().collect();
    let result = db.run_with_triggered(sql, params.clone(), triggered_std);
    assert_eq!(result[0], vec![CellValue::I64(1)], "inv_id should be 1 (condition 0 triggered)");
    assert_eq!(result[1], vec![CellValue::I64(0)], "inv_age should be 0 (condition 1 not triggered)");
    assert_eq!(result[2], vec![CellValue::Str("Alice".into())]);
}

#[test]
fn reactive_e2e_two_conditions_both_trigger() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv_id, REACTIVE(users.age > 30) AS inv_age, users.name FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    // 1. Plan + subscribe
    let ast = parser::parse(sql).expect("parse failed");
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).unwrap();
    let mut registry = SubscriptionRegistry::new();
    let sub_id = registry.subscribe(&plan.conditions, &plan.sources, &params).unwrap();

    // 2. Mutation: id=1, age=35 → both conditions trigger
    let mut zset = sql_engine::storage::ZSet::new();
    zset.insert("users".into(), vec![CellValue::I64(1), CellValue::Str("X".into()), CellValue::I64(35)]);
    let (affected, trace) = traced_on_zset(&registry, &zset);
    let triggered = affected.get(&sub_id).unwrap();
    assert!(triggered.contains(&0));
    assert!(triggered.contains(&1));
    assert_reactive_trace(&trace, "
OnZSet 1 mutations
  INSERT users [1, 'X', 35]
    Hash [1] --> Sub(0)
    Scan --> Sub(0)
    Verify 1/1 triggered
      Sub(0) Condition[0] (users.id = 1) --> true
      Sub(0) Condition[1] (users.age > 30) --> true
  Condition[0]: run=1/1 total=1/1
  Condition[1]: run=1/1 total=1/1
");

    // 3. Re-query with triggered {0, 1} → inv_id=1, inv_age=1
    let triggered_std: HashSet<usize> = triggered.iter().copied().collect();
    let result = db.run_with_triggered(sql, params.clone(), triggered_std);
    assert_eq!(result[0], vec![CellValue::I64(1)], "inv_id should be 1");
    assert_eq!(result[1], vec![CellValue::I64(1)], "inv_age should be 1");
    assert_eq!(result[2], vec![CellValue::Str("Alice".into())]);
}

#[test]
fn reactive_e2e_condition_idx_correctness() {
    // This test specifically verifies that condition_idx is correctly assigned
    // per REACTIVE() expression. Without the counter fix (condition_idx was
    // hardcoded to 0), this test would fail: triggering only condition 1 would
    // show inv_id=1 instead of inv_id=0.
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv_id, REACTIVE(users.age > 30) AS inv_age, users.name FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);

    // Only trigger condition 1 (age>30), NOT condition 0 (id=1)
    // This is the scenario that breaks with condition_idx: 0 hardcoded
    let triggered = HashSet::from([1usize]);
    let result = db.run_with_triggered(sql, params, triggered);
    assert_eq!(result[0], vec![CellValue::I64(0)], "inv_id should be 0 (condition 0 NOT in triggered set)");
    assert_eq!(result[1], vec![CellValue::I64(1)], "inv_age should be 1 (condition 1 IS in triggered set)");
}

// ── Reactive execution trace snapshots ─────────────────────────────────

fn assert_reactive_trace(actual: &str, expected: &str) {
    let actual = actual.trim_end();
    let expected = expected.trim();
    assert_eq!(actual, expected, "\n\n--- ACTUAL ---\n{actual}\n\n--- EXPECTED ---\n{expected}\n");
}

fn traced_on_zset(
    registry: &SubscriptionRegistry,
    zset: &sql_engine::storage::ZSet,
) -> (fnv::FnvHashMap<sql_engine::reactive::SubscriptionId, fnv::FnvHashSet<usize>>, String) {
    let mut ctx = sql_engine::reactive::execute::ReactiveContext::new();
    let affected = sql_engine::reactive::execute::on_zset_ctx(&mut ctx, registry, zset);
    (affected, ctx.pretty_print())
}

#[test]
fn reactive_trace_table_level() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id) AS inv FROM users";
    let (registry, _sub_id) = plan_and_subscribe(&db, sql, &HashMap::new());

    let mut zset = sql_engine::storage::ZSet::new();
    zset.insert("users".into(), vec![CellValue::I64(99), CellValue::Str("X".into()), CellValue::I64(20)]);
    let (_affected, trace) = traced_on_zset(&registry, &zset);

    assert_reactive_trace(&trace, "
OnZSet 1 mutations
  INSERT users [99, 'X', 20]
    Scan --> Sub(0)
    Verify 1/1 triggered
      Sub(0) Condition[0] (None) --> true
  Condition[0]: run=1/1 total=1/1
");
}

#[test]
fn reactive_trace_wrong_table() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid) AS inv FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);
    let (registry, _sub_id) = plan_and_subscribe(&db, sql, &params);

    let mut zset = sql_engine::storage::ZSet::new();
    zset.insert("orders".into(), vec![CellValue::I64(1), CellValue::I64(1), CellValue::I64(100)]);
    let (affected, trace) = traced_on_zset(&registry, &zset);

    assert!(affected.is_empty());
    assert_reactive_trace(&trace, "
OnZSet 1 mutations
  INSERT orders [1, 1, 100]
");
}

#[test]
fn reactive_trace_verify_filter_rejects() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = :uid AND users.age > 50) AS inv FROM users WHERE users.id = :uid";
    let params: execute::Params = HashMap::from([("uid".into(), ParamValue::Int(1))]);
    let (registry, _sub_id) = plan_and_subscribe(&db, sql, &params);

    // id=1 matches index lookup → candidate found, but age=30 fails verify (age > 50)
    let mut zset = sql_engine::storage::ZSet::new();
    zset.insert("users".into(), vec![CellValue::I64(1), CellValue::Str("X".into()), CellValue::I64(30)]);
    let (affected, trace) = traced_on_zset(&registry, &zset);

    assert!(affected.is_empty());
    assert_reactive_trace(&trace, "
OnZSet 1 mutations
  INSERT users [1, 'X', 30]
    Hash [1] --> Sub(0)
    Verify 0/1 triggered
      Sub(0) Condition[0] ((users.id = 1 AND users.age > 50)) --> false
  Condition[0]: run=1/0 total=1/0
");
}

// ── Tests: IN expansion ────────────────────────────────────────────────

#[test]
fn reactive_plan_snapshot_in() {
    let db = make_db();
    assert_reactive_plan(&reactive_plan(&db,
        "SELECT REACTIVE(users.id IN (1, 2, 3)) AS inv FROM users"), "
Reactive[0] table=users strategy=IndexLookup 3 sets: [users.id = 1], [users.id = 2], [users.id = 3]
  verify: users.id IN (1, 2, 3)
");
}

#[test]
fn reactive_plan_snapshot_eq_and_in() {
    let db = make_db();
    assert_reactive_plan(&reactive_plan(&db,
        "SELECT REACTIVE(users.name = 'Alice' AND users.id IN (1, 2)) AS inv FROM users"), "
Reactive[0] table=users strategy=IndexLookup 2 sets: [users.name = 'Alice', users.id = 1], [users.name = 'Alice', users.id = 2]
  verify: (users.name = 'Alice' AND users.id IN (1, 2))
");
}

#[test]
fn reactive_in_matching_value_triggers() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id IN (1, 3)) AS inv FROM users";
    let (registry, sub_id) = plan_and_subscribe(&db, sql, &HashMap::new());

    let affected = sql_engine::reactive::execute::on_insert(
        &registry, "users",
        &[CellValue::I64(1), CellValue::Str("X".into()), CellValue::I64(20)],
    );
    assert_eq!(affected, vec![sub_id]);

    let affected = sql_engine::reactive::execute::on_insert(
        &registry, "users",
        &[CellValue::I64(3), CellValue::Str("Y".into()), CellValue::I64(25)],
    );
    assert_eq!(affected, vec![sub_id]);
}

#[test]
fn reactive_in_non_matching_value_does_not_trigger() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id IN (1, 3)) AS inv FROM users";
    let (registry, _sub_id) = plan_and_subscribe(&db, sql, &HashMap::new());

    let affected = sql_engine::reactive::execute::on_insert(
        &registry, "users",
        &[CellValue::I64(2), CellValue::Str("Z".into()), CellValue::I64(30)],
    );
    assert!(affected.is_empty());
}

#[test]
fn reactive_in_with_eq_combined() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.name = 'Alice' AND users.id IN (1, 2)) AS inv FROM users";
    let (registry, sub_id) = plan_and_subscribe(&db, sql, &HashMap::new());

    let affected = sql_engine::reactive::execute::on_insert(
        &registry, "users",
        &[CellValue::I64(1), CellValue::Str("Alice".into()), CellValue::I64(30)],
    );
    assert_eq!(affected, vec![sub_id]);

    let affected = sql_engine::reactive::execute::on_insert(
        &registry, "users",
        &[CellValue::I64(3), CellValue::Str("Alice".into()), CellValue::I64(30)],
    );
    assert!(affected.is_empty());

    let affected = sql_engine::reactive::execute::on_insert(
        &registry, "users",
        &[CellValue::I64(1), CellValue::Str("Bob".into()), CellValue::I64(30)],
    );
    assert!(affected.is_empty());
}

#[test]
fn reactive_in_trace_shows_o1_lookups() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id IN (1, 2, 3)) AS inv FROM users";
    let (registry, _sub_id) = plan_and_subscribe(&db, sql, &HashMap::new());

    let mut zset = sql_engine::storage::ZSet::new();
    zset.insert("users".into(), vec![CellValue::I64(2), CellValue::Str("X".into()), CellValue::I64(20)]);
    let (affected, trace) = traced_on_zset(&registry, &zset);

    assert_eq!(affected.len(), 1);
    assert_reactive_trace(&trace, "
OnZSet 1 mutations
  INSERT users [2, 'X', 20]
    Hash [2] --> Sub(0)
    Verify 1/1 triggered
      Sub(0) Condition[0] (users.id IN (1, 2, 3)) --> true
  Condition[0]: run=1/1 total=1/1
");
}

#[test]
fn reactive_in_trace_no_match() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id IN (1, 2, 3)) AS inv FROM users";
    let (registry, _sub_id) = plan_and_subscribe(&db, sql, &HashMap::new());

    let mut zset = sql_engine::storage::ZSet::new();
    zset.insert("users".into(), vec![CellValue::I64(99), CellValue::Str("X".into()), CellValue::I64(20)]);
    let (affected, trace) = traced_on_zset(&registry, &zset);

    assert!(affected.is_empty());
    assert_reactive_trace(&trace, "
OnZSet 1 mutations
  INSERT users [99, 'X', 20]
    Hash [99] --> miss
");
}

#[test]
fn reactive_in_with_eq_trace() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.name = 'Alice' AND users.id IN (1, 2)) AS inv FROM users";
    let (registry, _sub_id) = plan_and_subscribe(&db, sql, &HashMap::new());

    let mut zset = sql_engine::storage::ZSet::new();
    zset.insert("users".into(), vec![CellValue::I64(1), CellValue::Str("Alice".into()), CellValue::I64(30)]);
    let (affected, trace) = traced_on_zset(&registry, &zset);

    assert_eq!(affected.len(), 1);
    assert_reactive_trace(&trace, "
OnZSet 1 mutations
  INSERT users [1, 'Alice', 30]
    Hash [1, 'Alice'] --> Sub(0)
    Verify 1/1 triggered
      Sub(0) Condition[0] ((users.name = 'Alice' AND users.id IN (1, 2))) --> true
  Condition[0]: run=1/1 total=1/1
");
}

#[test]
fn reactive_in_unsubscribe_cleans_all_keys() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id IN (1, 2, 3)) AS inv FROM users";
    let ast = parser::parse(sql).expect("parse failed");
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).unwrap();
    let mut registry = SubscriptionRegistry::new();
    let sub_id = registry.subscribe(&plan.conditions, &plan.sources, &HashMap::new()).unwrap();

    assert_eq!(registry.reverse_index_size(), 3);
    assert_eq!(registry.subscription_count(), 1);

    registry.unsubscribe(sub_id);
    assert_eq!(registry.reverse_index_size(), 0);
    assert_eq!(registry.subscription_count(), 0);
}

#[test]
fn reactive_in_multiple_subscriptions_share_keys() {
    let db = make_db();
    let sql1 = "SELECT REACTIVE(users.id IN (1, 2)) AS inv FROM users";
    let sql2 = "SELECT REACTIVE(users.id IN (2, 3)) AS inv FROM users";

    let ast1 = parser::parse(sql1).expect("parse failed");
    let plan1 = sql_engine::planner::reactive::plan_reactive(&ast1, &db.table_schemas, &db.callers.requirements).unwrap();
    let ast2 = parser::parse(sql2).expect("parse failed");
    let plan2 = sql_engine::planner::reactive::plan_reactive(&ast2, &db.table_schemas, &db.callers.requirements).unwrap();

    let mut registry = SubscriptionRegistry::new();
    let sub1 = registry.subscribe(&plan1.conditions, &plan1.sources, &HashMap::new()).unwrap();
    let sub2 = registry.subscribe(&plan2.conditions, &plan2.sources, &HashMap::new()).unwrap();

    let affected = sql_engine::reactive::execute::on_insert(
        &registry, "users",
        &[CellValue::I64(2), CellValue::Str("X".into()), CellValue::I64(20)],
    );
    assert_eq!(affected.len(), 2);
    assert!(affected.contains(&sub1));
    assert!(affected.contains(&sub2));

    let affected = sql_engine::reactive::execute::on_insert(
        &registry, "users",
        &[CellValue::I64(1), CellValue::Str("Y".into()), CellValue::I64(25)],
    );
    assert_eq!(affected, vec![sub1]);

    let affected = sql_engine::reactive::execute::on_insert(
        &registry, "users",
        &[CellValue::I64(3), CellValue::Str("Z".into()), CellValue::I64(30)],
    );
    assert_eq!(affected, vec![sub2]);
}

#[test]
fn reactive_in_with_range_trace() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id IN (1, 2) AND users.age > 25) AS inv FROM users";
    let (registry, _sub_id) = plan_and_subscribe(&db, sql, &HashMap::new());

    let mut zset = sql_engine::storage::ZSet::new();
    zset.insert("users".into(), vec![CellValue::I64(1), CellValue::Str("X".into()), CellValue::I64(30)]);
    let (affected, trace) = traced_on_zset(&registry, &zset);

    assert_eq!(affected.len(), 1);
    assert_reactive_trace(&trace, "
OnZSet 1 mutations
  INSERT users [1, 'X', 30]
    Hash [1] --> Sub(0)
    Verify 1/1 triggered
      Sub(0) Condition[0] ((users.id IN (1, 2) AND users.age > 25)) --> true
  Condition[0]: run=1/1 total=1/1
");
}

#[test]
fn reactive_in_with_range_verify_rejects() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id IN (1, 2) AND users.age > 25) AS inv FROM users";
    let (registry, _sub_id) = plan_and_subscribe(&db, sql, &HashMap::new());

    let mut zset = sql_engine::storage::ZSet::new();
    zset.insert("users".into(), vec![CellValue::I64(1), CellValue::Str("X".into()), CellValue::I64(20)]);
    let (affected, trace) = traced_on_zset(&registry, &zset);

    assert!(affected.is_empty());
    assert_reactive_trace(&trace, "
OnZSet 1 mutations
  INSERT users [1, 'X', 20]
    Hash [1] --> Sub(0)
    Verify 0/1 triggered
      Sub(0) Condition[0] ((users.id IN (1, 2) AND users.age > 25)) --> false
  Condition[0]: run=1/0 total=1/0
");
}

// ── Tests: OR expansion (OR→IN normalization in the reactive optimizer) ──
//
// `col = A OR col = B` is semantically equivalent to `col IN (A, B)`. The
// reactive optimizer normalizes OR-chains on the same column into IN and then
// reuses the IN-expansion path (multiple hash-index lookups). Pretty-printing
// shows the rewritten form.

#[test]
fn reactive_plan_snapshot_or_same_column() {
    let db = make_db();
    assert_reactive_plan(&reactive_plan(&db,
        "SELECT REACTIVE(users.id = 1 OR users.id = 2) AS inv FROM users"), "
Reactive[0] table=users strategy=IndexLookup 2 sets: [users.id = 1], [users.id = 2]
  verify: users.id IN (1, 2)
");
}

#[test]
fn reactive_plan_snapshot_or_three_values() {
    let db = make_db();
    assert_reactive_plan(&reactive_plan(&db,
        "SELECT REACTIVE(users.id = 1 OR users.id = 2 OR users.id = 3) AS inv FROM users"), "
Reactive[0] table=users strategy=IndexLookup 3 sets: [users.id = 1], [users.id = 2], [users.id = 3]
  verify: users.id IN (1, 2, 3)
");
}

#[test]
fn reactive_plan_snapshot_or_mixed_with_in() {
    let db = make_db();
    assert_reactive_plan(&reactive_plan(&db,
        "SELECT REACTIVE(users.id IN (1, 2) OR users.id = 3) AS inv FROM users"), "
Reactive[0] table=users strategy=IndexLookup 3 sets: [users.id = 1], [users.id = 2], [users.id = 3]
  verify: users.id IN (1, 2, 3)
");
}

#[test]
fn reactive_plan_snapshot_or_different_columns_stays_scan() {
    // Cannot merge across columns — OR is preserved, strategy falls back.
    let db = make_db();
    assert_reactive_plan(&reactive_plan(&db,
        "SELECT REACTIVE(users.id = 1 OR users.name = 'X') AS inv FROM users"), "
Reactive[0] table=users strategy=TableScan
  verify: (users.id = 1 OR users.name = 'X')
");
}

#[test]
fn reactive_plan_snapshot_or_inside_and() {
    // (id = 1 OR id = 2) AND name = 'Alice' → Cartesian: 2 composite sets.
    let db = make_db();
    assert_reactive_plan(&reactive_plan(&db,
        "SELECT REACTIVE((users.id = 1 OR users.id = 2) AND users.name = 'Alice') AS inv FROM users"), "
Reactive[0] table=users strategy=IndexLookup 2 sets: [users.name = 'Alice', users.id = 1], [users.name = 'Alice', users.id = 2]
  verify: (users.id IN (1, 2) AND users.name = 'Alice')
");
}

#[test]
fn reactive_plan_snapshot_or_with_range_leaf_stays_scan() {
    // One leaf is not an equality → whole OR stays, strategy is TableScan.
    let db = make_db();
    assert_reactive_plan(&reactive_plan(&db,
        "SELECT REACTIVE(users.id = 1 OR users.age > 30) AS inv FROM users"), "
Reactive[0] table=users strategy=TableScan
  verify: (users.id = 1 OR users.age > 30)
");
}

#[test]
fn reactive_plan_snapshot_or_with_placeholder() {
    let db = make_db();
    assert_reactive_plan(&reactive_plan(&db,
        "SELECT REACTIVE(users.id = :a OR users.id = :b) AS inv FROM users"), "
Reactive[0] table=users strategy=IndexLookup 2 sets: [users.id = :a], [users.id = :b]
  verify: users.id IN (:a, :b)
");
}

#[test]
fn reactive_or_matching_value_triggers() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = 1 OR users.id = 3) AS inv FROM users";
    let (registry, sub_id) = plan_and_subscribe(&db, sql, &HashMap::new());

    let affected = sql_engine::reactive::execute::on_insert(
        &registry, "users",
        &[CellValue::I64(1), CellValue::Str("X".into()), CellValue::I64(20)],
    );
    assert_eq!(affected, vec![sub_id]);

    let affected = sql_engine::reactive::execute::on_insert(
        &registry, "users",
        &[CellValue::I64(3), CellValue::Str("Y".into()), CellValue::I64(25)],
    );
    assert_eq!(affected, vec![sub_id]);
}

#[test]
fn reactive_or_non_matching_value_does_not_trigger() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = 1 OR users.id = 3) AS inv FROM users";
    let (registry, _sub_id) = plan_and_subscribe(&db, sql, &HashMap::new());

    let affected = sql_engine::reactive::execute::on_insert(
        &registry, "users",
        &[CellValue::I64(2), CellValue::Str("Z".into()), CellValue::I64(30)],
    );
    assert!(affected.is_empty());
}

#[test]
fn reactive_or_trace_shows_hash_lookup() {
    // OR-chain on same column should use an O(1) hash lookup, not a scan.
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = 1 OR users.id = 2 OR users.id = 3) AS inv FROM users";
    let (registry, _sub_id) = plan_and_subscribe(&db, sql, &HashMap::new());

    let mut zset = sql_engine::storage::ZSet::new();
    zset.insert("users".into(), vec![CellValue::I64(2), CellValue::Str("X".into()), CellValue::I64(20)]);
    let (affected, trace) = traced_on_zset(&registry, &zset);

    assert_eq!(affected.len(), 1);
    assert_reactive_trace(&trace, "
OnZSet 1 mutations
  INSERT users [2, 'X', 20]
    Hash [2] --> Sub(0)
    Verify 1/1 triggered
      Sub(0) Condition[0] (users.id IN (1, 2, 3)) --> true
  Condition[0]: run=1/1 total=1/1
");
}

#[test]
fn reactive_or_trace_no_match() {
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = 1 OR users.id = 2 OR users.id = 3) AS inv FROM users";
    let (registry, _sub_id) = plan_and_subscribe(&db, sql, &HashMap::new());

    let mut zset = sql_engine::storage::ZSet::new();
    zset.insert("users".into(), vec![CellValue::I64(99), CellValue::Str("X".into()), CellValue::I64(20)]);
    let (affected, trace) = traced_on_zset(&registry, &zset);

    assert!(affected.is_empty());
    assert_reactive_trace(&trace, "
OnZSet 1 mutations
  INSERT users [99, 'X', 20]
    Hash [99] --> miss
");
}

#[test]
fn reactive_or_different_columns_falls_back_to_scan() {
    // OR across different columns cannot be merged → TableScan trace.
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = 1 OR users.name = 'Alice') AS inv FROM users";
    let (registry, sub_id) = plan_and_subscribe(&db, sql, &HashMap::new());

    // Row matching the OR on name → still triggers via scan + verify.
    let mut zset = sql_engine::storage::ZSet::new();
    zset.insert("users".into(), vec![CellValue::I64(99), CellValue::Str("Alice".into()), CellValue::I64(20)]);
    let (affected, trace) = traced_on_zset(&registry, &zset);

    let expected: fnv::FnvHashMap<_, fnv::FnvHashSet<usize>> = std::iter::once((sub_id, [0usize].into_iter().collect())).collect();
    assert_eq!(affected, expected);
    assert_reactive_trace(&trace, "
OnZSet 1 mutations
  INSERT users [99, 'Alice', 20]
    Scan --> Sub(0)
    Verify 1/1 triggered
      Sub(0) Condition[0] ((users.id = 1 OR users.name = 'Alice')) --> true
  Condition[0]: run=1/1 total=1/1
");
}

#[test]
fn reactive_or_and_eq_combined() {
    // (id = 1 OR id = 2) AND name = 'Alice' — composite hash lookup.
    let db = make_db();
    let sql = "SELECT REACTIVE((users.id = 1 OR users.id = 2) AND users.name = 'Alice') AS inv FROM users";
    let (registry, sub_id) = plan_and_subscribe(&db, sql, &HashMap::new());

    // Matches: id=1, name=Alice
    let affected = sql_engine::reactive::execute::on_insert(
        &registry, "users",
        &[CellValue::I64(1), CellValue::Str("Alice".into()), CellValue::I64(30)],
    );
    assert_eq!(affected, vec![sub_id]);

    // Wrong id
    let affected = sql_engine::reactive::execute::on_insert(
        &registry, "users",
        &[CellValue::I64(3), CellValue::Str("Alice".into()), CellValue::I64(30)],
    );
    assert!(affected.is_empty());

    // Wrong name
    let affected = sql_engine::reactive::execute::on_insert(
        &registry, "users",
        &[CellValue::I64(1), CellValue::Str("Bob".into()), CellValue::I64(30)],
    );
    assert!(affected.is_empty());
}

#[test]
fn reactive_or_unsubscribe_cleans_all_keys() {
    // A 3-value OR must register 3 reverse-index entries — all cleaned on unsubscribe.
    let db = make_db();
    let sql = "SELECT REACTIVE(users.id = 1 OR users.id = 2 OR users.id = 3) AS inv FROM users";
    let ast = parser::parse(sql).expect("parse failed");
    let plan = sql_engine::planner::reactive::plan_reactive(&ast, &db.table_schemas, &db.callers.requirements).unwrap();
    let mut registry = SubscriptionRegistry::new();
    let sub_id = registry.subscribe(&plan.conditions, &plan.sources, &HashMap::new()).unwrap();

    assert_eq!(registry.reverse_index_size(), 3);
    assert_eq!(registry.subscription_count(), 1);

    registry.unsubscribe(sub_id);
    assert_eq!(registry.reverse_index_size(), 0);
    assert_eq!(registry.subscription_count(), 0);
}

#[test]
fn reactive_or_multiple_subscriptions_share_keys() {
    // Sub1 watches id in {1,2}, Sub2 watches id in {2,3} (via OR) → id=2 hits both.
    let db = make_db();
    let sql1 = "SELECT REACTIVE(users.id = 1 OR users.id = 2) AS inv FROM users";
    let sql2 = "SELECT REACTIVE(users.id = 2 OR users.id = 3) AS inv FROM users";

    let ast1 = parser::parse(sql1).expect("parse failed");
    let plan1 = sql_engine::planner::reactive::plan_reactive(&ast1, &db.table_schemas, &db.callers.requirements).unwrap();
    let ast2 = parser::parse(sql2).expect("parse failed");
    let plan2 = sql_engine::planner::reactive::plan_reactive(&ast2, &db.table_schemas, &db.callers.requirements).unwrap();

    let mut registry = SubscriptionRegistry::new();
    let sub1 = registry.subscribe(&plan1.conditions, &plan1.sources, &HashMap::new()).unwrap();
    let sub2 = registry.subscribe(&plan2.conditions, &plan2.sources, &HashMap::new()).unwrap();

    let affected = sql_engine::reactive::execute::on_insert(
        &registry, "users",
        &[CellValue::I64(2), CellValue::Str("X".into()), CellValue::I64(20)],
    );
    assert_eq!(affected.len(), 2);
    assert!(affected.contains(&sub1));
    assert!(affected.contains(&sub2));

    let affected = sql_engine::reactive::execute::on_insert(
        &registry, "users",
        &[CellValue::I64(1), CellValue::Str("Y".into()), CellValue::I64(25)],
    );
    assert_eq!(affected, vec![sub1]);

    let affected = sql_engine::reactive::execute::on_insert(
        &registry, "users",
        &[CellValue::I64(3), CellValue::Str("Z".into()), CellValue::I64(30)],
    );
    assert_eq!(affected, vec![sub2]);
}
