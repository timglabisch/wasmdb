//! E2E: Trait-based registration (`DbTable` + `DbCaller`).
//!
//! Manually implements both traits — this is the exact shape
//! `tables-macros` (DbTable) and `tables-codegen` (DbCaller) will
//! emit. Exercises the `Database::register_table` +
//! `Database::register_caller_of` path end to end against the async
//! SQL pipeline (Phase 0 fetcher → Phase 3 scan).

use std::sync::Arc;

use database::Database;
use sql_engine::execute::FetcherFuture;
use sql_engine::planner::requirement::{RequirementMeta, RequirementParamDef};
use sql_engine::schema::{ColumnSchema, DataType, TableSchema};
use sql_engine::storage::CellValue;
use sql_engine::{DbCaller, DbTable};
use sql_parser::ast::Value;

// ── User types (what `#[row]` / `#[query]` would produce) ─────────────

#[derive(Clone)]
struct Customer {
    id: i64,
    name: String,
}

impl DbTable for Customer {
    const TABLE: &'static str = "customer";

    fn schema() -> TableSchema {
        TableSchema {
            name: Self::TABLE.into(),
            columns: vec![
                ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "name".into(), data_type: DataType::String, nullable: false },
            ],
            primary_key: vec![0],
            indexes: vec![],
        }
    }

    fn into_cells(self) -> Vec<CellValue> {
        vec![CellValue::I64(self.id), CellValue::Str(self.name)]
    }
}

struct AppCtx {
    // In a real server this would be a pool/handle; here we just carry
    // a fixture so we can prove the ctx actually reaches `call`.
    fixtures: Vec<Customer>,
}

struct ByOwner;

impl DbCaller for ByOwner {
    const ID: &'static str = "customer::by_owner";
    type Ctx = AppCtx;
    type Row = Customer;

    fn meta() -> RequirementMeta {
        RequirementMeta {
            row_table: <Self::Row as DbTable>::TABLE.into(),
            params: vec![RequirementParamDef {
                name: "owner_id".into(),
                data_type: DataType::I64,
            }],
        }
    }

    fn call(args: Vec<Value>, ctx: Arc<Self::Ctx>) -> FetcherFuture {
        Box::pin(async move {
            let owner_id: i64 = match args.first() {
                Some(Value::Int(n)) => *n,
                _ => return Err("arg 0 (owner_id): expected Int".into()),
            };
            let rows = ctx
                .fixtures
                .iter()
                .filter(|_| owner_id == 1)
                .cloned()
                .map(<Customer as DbTable>::into_cells)
                .collect();
            Ok(rows)
        })
    }
}

// ── Test ──────────────────────────────────────────────────────────────

#[test]
fn register_table_and_caller_resolve_async() {
    let mut db = Database::new();
    db.register_table::<Customer>().unwrap();

    let ctx = Arc::new(AppCtx {
        fixtures: vec![
            Customer { id: 1, name: "Alice".into() },
            Customer { id: 2, name: "Bob".into() },
        ],
    });
    db.register_caller_of::<ByOwner>(ctx);

    let cols = pollster::block_on(
        db.execute_async("SELECT customer.name FROM customer.by_owner(1)"),
    )
    .expect("execute_async");

    // `Columns` is column-major: one outer entry per projected column.
    assert_eq!(cols.len(), 1, "expected one projected column");
    let name_col = &cols[0];
    assert!(name_col.contains(&CellValue::Str("Alice".into())));
    assert!(name_col.contains(&CellValue::Str("Bob".into())));
    assert_eq!(name_col.len(), 2);
}

#[test]
fn unknown_owner_yields_empty_result() {
    let mut db = Database::new();
    db.register_table::<Customer>().unwrap();
    db.register_caller_of::<ByOwner>(Arc::new(AppCtx {
        fixtures: vec![Customer { id: 1, name: "Alice".into() }],
    }));

    let cols = pollster::block_on(
        db.execute_async("SELECT customer.name FROM customer.by_owner(999)"),
    )
    .expect("execute_async");

    assert_eq!(cols.len(), 1, "expected one projected column");
    assert!(cols[0].is_empty(), "expected no rows, got {:?}", cols[0]);
}
