//! JOIN variants through the trait/caller path.
//!
//! The plain-table side uses `INSERT INTO customer ...` to seed rows into
//! the `Database`-registered table (via `DbTable`). The caller side comes
//! from `invoices.by_customer(...)`.

mod common;

use common::{check_plans, i, run, s, setup_db};
use tables_e2e::AppCtx;

fn seed_customer_table(db: &mut database::Database) {
    // Seed the plain-table view of `customer` (the `register_table::<Customer>`
    // schema) so we can JOIN it against a caller-sourced `invoices.by_customer`.
    for sql in [
        "INSERT INTO customer (id, name, owner_id) VALUES (1, 'Alice', 1)",
        "INSERT INTO customer (id, name, owner_id) VALUES (2, 'Bob', 2)",
        "INSERT INTO customer (id, name, owner_id) VALUES (3, 'Carol', 1)",
    ] {
        pollster::block_on(db.execute_async(sql)).expect("insert customer");
    }
}

#[test]
fn inner_join_caller_and_plain_table() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    seed_customer_table(&mut db);

    let sql = "SELECT customer.name, invoice.amount \
         FROM customer \
         INNER JOIN invoices.by_customer(1) ON customer.id = invoice.customer_id";
    let cols = run(&mut db, sql);
    // Alice (id=1) has invoices 100 + 200; the caller emits only customer 1.
    assert_eq!(cols[0], vec![s("Alice"), s("Alice")]);
    assert_eq!(cols[1], vec![i(100), i(200)]);
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller invoices::by_customer(1) row=invoices
=== ExecutionPlan ===
Select
  Scan table=customer scan=Full
  Join type=Inner strategy=NestedLoop table=invoice caller=invoices::by_customer row=invoice args=[:__caller_1_arg_0]
    on: customer.id = invoice.customer_id
  Output [customer.name, invoice.amount]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn inner_join_caller_first_then_plain_table() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    seed_customer_table(&mut db);

    let sql = "SELECT customer.name, invoice.amount \
         FROM invoices.by_customer(2) \
         INNER JOIN customer ON customer.id = invoice.customer_id";
    let cols = run(&mut db, sql);
    assert_eq!(cols[0], vec![s("Bob")]);
    assert_eq!(cols[1], vec![i(50)]);
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller invoices::by_customer(2) row=invoices
=== ExecutionPlan ===
Select
  Scan table=invoice caller=invoices::by_customer row=invoice args=[:__caller_0_arg_0]
  Join type=Inner strategy=IndexLookup(Hash[0]) table=customer scan=Full
    on: customer.id = invoice.customer_id
  Output [customer.name, invoice.amount]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn left_join_caller_plain_table_fills_null() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    seed_customer_table(&mut db);

    let sql = "SELECT customer.name, invoice.amount \
         FROM customer \
         LEFT JOIN invoices.by_customer(1) ON customer.id = invoice.customer_id";
    let cols = run(&mut db, sql);
    // Alice → 2 rows (matched), Bob + Carol → 1 row each (right side NULL).
    assert_eq!(cols[0].len(), 4);
    // Null amount shows up for the customers whose ids don't match customer_id=1.
    let null_count = cols[1]
        .iter()
        .filter(|v| matches!(v, sql_engine::storage::CellValue::Null))
        .count();
    assert_eq!(null_count, 2);
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller invoices::by_customer(1) row=invoices
=== ExecutionPlan ===
Select
  Scan table=customer scan=Full
  Join type=Left strategy=NestedLoop table=invoice caller=invoices::by_customer row=invoice args=[:__caller_1_arg_0]
    on: customer.id = invoice.customer_id
  Output [customer.name, invoice.amount]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}

#[test]
fn inner_join_caller_and_plain_table_with_where() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    seed_customer_table(&mut db);

    let sql = "SELECT customer.name, invoice.amount \
         FROM customer \
         INNER JOIN invoices.by_customer(1) ON customer.id = invoice.customer_id \
         WHERE invoice.amount > 100";
    let cols = run(&mut db, sql);
    assert_eq!(cols[0], vec![s("Alice")]);
    assert_eq!(cols[1], vec![i(200)]);
    check_plans(
        &db,
        sql,
        "\
=== RequirementPlan ===
RequirementPlan (1 requirements)
  [0] Caller invoices::by_customer(1) row=invoices
=== ExecutionPlan ===
Select
  Scan table=customer scan=Full
  Join type=Inner strategy=NestedLoop table=invoice caller=invoices::by_customer row=invoice args=[:__caller_1_arg_0]
    pre_filter: invoice.amount > 100
    on: customer.id = invoice.customer_id
  Output [customer.name, invoice.amount]
=== ReactivePlan ===
ReactivePlan (no conditions)",
    );
}
