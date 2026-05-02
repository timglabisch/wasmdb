/// Minimal example that avoids HashMap entirely — just to measure the size delta.
use sql_engine::storage::{CellValue, Table};
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
    table.insert(&[CellValue::I64(3), CellValue::Str("Carol".into()), CellValue::I64(35)]).unwrap();

    table.get(0, 0);
    table.get(1, 1);

    3
}

fn main() {
    let n = run();
    println!("rows: {n}");
}
