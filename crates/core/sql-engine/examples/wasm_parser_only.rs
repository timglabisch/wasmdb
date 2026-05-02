/// Parser + Storage, aber kein HashMap (kein execute/planner).
use sql_engine::storage::{CellValue, Table};
use sql_parser::parser;
use sql_engine::schema::{ColumnSchema, DataType, TableSchema};

#[no_mangle]
pub extern "C" fn run() -> i64 {
    let schema = TableSchema {
        name: "users".into(),
        columns: vec![
            ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
            ColumnSchema { name: "name".into(), data_type: DataType::String, nullable: false },
            ColumnSchema { name: "age".into(), data_type: DataType::I64, nullable: true },
        ],
        primary_key: vec![0],
        indexes: vec![],
    };

    let mut table = Table::new(schema);
    table.insert(&[CellValue::I64(1), CellValue::Str("Alice".into()), CellValue::I64(30)]).unwrap();
    table.insert(&[CellValue::I64(2), CellValue::Str("Bob".into()), CellValue::I64(25)]).unwrap();

    let ast = parser::parse("SELECT users.name FROM await(customers.by_reference('df')) WHERE users.age > 28").unwrap();
    ast.result_columns.len() as i64
}

fn main() {
    let n = run();
    println!("{n}");
}
