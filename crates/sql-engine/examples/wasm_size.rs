use std::collections::HashMap;

use sql_engine::execute;
use sql_engine::planner;
use sql_engine::storage::{CellValue, Table};
use sql_parser::parser;
use ddl_parser::schema::{ColumnSchema, DataType, TableSchema};

#[no_mangle]
pub extern "C" fn run() -> i64 {
    let table_schema = TableSchema {
        name: "users".into(),
        columns: vec![
            ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
            ColumnSchema { name: "name".into(), data_type: DataType::String, nullable: false },
            ColumnSchema { name: "age".into(), data_type: DataType::I64, nullable: true },
        ],
        primary_key: vec![0],
        indexes: vec![],
    };

    let mut table = Table::new(table_schema.clone());
    table.insert(&[CellValue::I64(1), CellValue::Str("Alice".into()), CellValue::I64(30)]).unwrap();
    table.insert(&[CellValue::I64(2), CellValue::Str("Bob".into()), CellValue::I64(25)]).unwrap();
    table.insert(&[CellValue::I64(3), CellValue::Str("Carol".into()), CellValue::I64(35)]).unwrap();

    let mut tables = HashMap::new();
    tables.insert("users".into(), table);

    let mut table_schemas = HashMap::new();
    table_schemas.insert("users".into(), table_schema);

    let ast = parser::parse("SELECT users.name, users.age FROM users WHERE users.age > 28").unwrap();
    let plan = planner::plan_select(&ast, &table_schemas).unwrap();
    let mut ctx = execute::ExecutionContext::new();
    let result = execute::execute(&mut ctx, &plan, &tables).unwrap();

    result[0].len() as i64
}

fn main() {
    let n = run();
    println!("rows: {n}");
}
