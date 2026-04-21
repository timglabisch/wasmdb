//! GROUP BY + COUNT / SUM / MIN / MAX over caller output.

mod common;

use common::{i, run, s, setup_db};
use tables_e2e::AppCtx;

#[test]
fn count_over_caller_grouped() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT invoice.customer_id, COUNT(invoice.id) \
         FROM invoices.min_amount(0) \
         GROUP BY invoice.customer_id",
    );
    // Alice(1) has 2 invoices; Bob(2) has 1; Carol(3) has 1.
    assert_eq!(cols[0].len(), 3);
    let pairs: Vec<(i64, i64)> = cols[0]
        .iter()
        .zip(cols[1].iter())
        .map(|(k, v)| match (k, v) {
            (sql_engine::storage::CellValue::I64(a), sql_engine::storage::CellValue::I64(b)) => {
                (*a, *b)
            }
            _ => panic!("unexpected cell shape"),
        })
        .collect();
    assert!(pairs.contains(&(1, 2)));
    assert!(pairs.contains(&(2, 1)));
    assert!(pairs.contains(&(3, 1)));
}

#[test]
fn sum_over_caller_grouped() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT invoice.customer_id, SUM(invoice.amount) \
         FROM invoices.min_amount(0) \
         GROUP BY invoice.customer_id",
    );
    // Alice=300, Bob=50, Carol=300.
    let pairs: Vec<(i64, i64)> = cols[0]
        .iter()
        .zip(cols[1].iter())
        .map(|(k, v)| match (k, v) {
            (sql_engine::storage::CellValue::I64(a), sql_engine::storage::CellValue::I64(b)) => {
                (*a, *b)
            }
            _ => panic!("unexpected cell shape"),
        })
        .collect();
    assert!(pairs.contains(&(1, 300)));
    assert!(pairs.contains(&(2, 50)));
    assert!(pairs.contains(&(3, 300)));
}

#[test]
fn min_max_over_caller_grouped() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT invoice.customer_id, MIN(invoice.amount), MAX(invoice.amount) \
         FROM invoices.min_amount(0) \
         GROUP BY invoice.customer_id",
    );
    // Alice: min=100, max=200
    let row_alice = cols[0]
        .iter()
        .position(|v| v == &i(1))
        .expect("alice group");
    assert_eq!(cols[1][row_alice], i(100));
    assert_eq!(cols[2][row_alice], i(200));
}

#[test]
fn count_skips_null_on_option_column() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT invoice.customer_id, COUNT(invoice.note) \
         FROM invoices.min_amount(0) \
         GROUP BY invoice.customer_id",
    );
    // Alice has notes [Some('rush'), None] → COUNT=1
    // Carol has note=None → COUNT=0
    let row_alice = cols[0]
        .iter()
        .position(|v| v == &i(1))
        .expect("alice group");
    assert_eq!(cols[1][row_alice], i(1));
    let row_carol = cols[0]
        .iter()
        .position(|v| v == &i(3))
        .expect("carol group");
    assert_eq!(cols[1][row_carol], i(0));
}

#[test]
fn aggregate_after_caller_plaintable_join() {
    // Seed a plain `customer` table; join with invoice caller; group by name.
    let mut db = setup_db(AppCtx::with_default_fixtures());
    for sql in [
        "INSERT INTO customer (id, name, owner_id) VALUES (1, 'Alice', 1)",
        "INSERT INTO customer (id, name, owner_id) VALUES (2, 'Bob', 2)",
        "INSERT INTO customer (id, name, owner_id) VALUES (3, 'Carol', 1)",
    ] {
        pollster::block_on(db.execute_async(sql)).expect("insert customer");
    }

    let cols = run(
        &mut db,
        "SELECT customer.name, SUM(invoice.amount) \
         FROM customer \
         INNER JOIN invoices.min_amount(0) ON customer.id = invoice.customer_id \
         GROUP BY customer.name",
    );
    // Alice=300, Bob=50, Carol=300.
    assert_eq!(cols[0].len(), 3);
    assert!(cols[0].contains(&s("Alice")));
    assert!(cols[0].contains(&s("Bob")));
    assert!(cols[0].contains(&s("Carol")));
}
