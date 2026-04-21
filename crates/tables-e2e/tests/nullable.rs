//! NULL semantics end-to-end — Option<String>/Option<i64> columns survive
//! the caller → Database pipeline unchanged.

mod common;

use common::{i, run, s, setup_db};
use sql_engine::storage::CellValue;
use tables_e2e::AppCtx;

#[test]
fn option_none_in_caller_output_reaches_as_null() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT invoice.id, invoice.note FROM invoices.min_amount(0) ORDER BY invoice.id",
    );
    // Fixtures:
    //   10 rush, 11 NULL, 12 urgent rush, 13 NULL.
    assert_eq!(cols[0], vec![i(10), i(11), i(12), i(13)]);
    assert_eq!(
        cols[1],
        vec![
            s("rush"),
            CellValue::Null,
            s("urgent rush"),
            CellValue::Null,
        ],
    );
}

#[test]
fn option_some_in_caller_output_preserved() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    let cols = run(
        &mut db,
        "SELECT product.sku, product.price FROM products.cheaper_than(1000) ORDER BY product.price",
    );
    // cheaper_than(1000) only returns products with Some(price) below 1000:
    // widget(50), gadget(100). Freebie has None → filtered out.
    assert_eq!(cols[0], vec![s("widget"), s("gadget")]);
    assert_eq!(cols[1], vec![i(50), i(100)]);
}

// NOTE: `IS NULL` / `IS NOT NULL` predicates are not yet implemented in
// `sql-parser` (TokenKind::Is exists but no binary-op binding). When that
// lands, add regression tests here:
//   * WHERE invoice.note IS NULL     → vec![i(11), i(13)]
//   * WHERE invoice.note IS NOT NULL → vec![i(10), i(12)]
// Until then, NULL propagation is covered by
// `option_none_in_caller_output_reaches_as_null` above.

#[test]
fn null_literal_arg_to_option_caller() {
    let mut db = setup_db(AppCtx::with_default_fixtures());
    // `with_optional_note(NULL)` → Option<String>::None → matches invoices
    // whose note column is `None`.
    let cols = run(
        &mut db,
        "SELECT invoice.id FROM invoices.with_optional_note(NULL) ORDER BY invoice.id",
    );
    assert_eq!(cols[0], vec![i(11), i(13)]);
}
