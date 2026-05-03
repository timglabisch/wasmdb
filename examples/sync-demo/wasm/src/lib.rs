//! cdylib-only crate. The full wasm-bindgen surface (subscribe,
//! query, debug exports, init/execute/...) lives in
//! `sync_client::wasm`; this crate only registers the demo's tables
//! and stamps the macro with `UserCommand`.

#[cfg(target_arch = "wasm32")]
mod app {
    use database::Database;
    use sql_engine::schema::{ColumnSchema, DataType, IndexSchema, IndexType, TableSchema};
    use sync_demo_commands::UserCommand;

    fn setup_db(db: &mut Database) {
        db.create_table(TableSchema {
            name: "users".into(),
            columns: vec![
                ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "name".into(), data_type: DataType::String, nullable: false },
                ColumnSchema { name: "age".into(), data_type: DataType::I64, nullable: false },
            ],
            primary_key: vec![0],
            indexes: vec![],
        }).unwrap();
        db.create_table(TableSchema {
            name: "orders".into(),
            columns: vec![
                ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "user_id".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "amount".into(), data_type: DataType::I64, nullable: false },
                ColumnSchema { name: "status".into(), data_type: DataType::String, nullable: false },
            ],
            primary_key: vec![0],
            indexes: vec![
                IndexSchema { name: None, columns: vec![1], index_type: IndexType::BTree },
            ],
        }).unwrap();
    }

    sync_client::define_wasm_api!(
        command = UserCommand,
        setup_db = setup_db,
    );
}
