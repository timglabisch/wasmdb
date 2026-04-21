//! Smoke test — proves the wiring works end-to-end: generated `DbTable`
//! schema + generated `DbCaller::call` + `Database::execute_async`.

mod common;

use common::{run, s, setup_db};
use tables_e2e::AppCtx;

#[test]
fn select_name_from_by_owner_caller_returns_rows() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(&mut db, "SELECT customer.name FROM customers.by_owner(1)");

    assert_eq!(cols.len(), 1, "one projected column");
    let name_col = &cols[0];
    assert!(name_col.contains(&s("Alice")));
    assert!(name_col.contains(&s("Carol")));
    assert_eq!(name_col.len(), 2, "owner 1 has 2 customers");
}
