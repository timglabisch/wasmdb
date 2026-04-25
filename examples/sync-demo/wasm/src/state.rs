use std::cell::RefCell;

use database::Database;
use sync_demo_commands::UserCommand;
use sql_engine::schema::{ColumnSchema, DataType, IndexSchema, IndexType, TableSchema};
use sync_client::client::SyncClient;

thread_local! {
    static CLIENT: RefCell<Option<SyncClient<UserCommand>>> = RefCell::new(None);
    pub(crate) static DEFAULT_STREAM_ID: RefCell<Option<u64>> = RefCell::new(None);
}

pub(crate) fn install_client(client: SyncClient<UserCommand>) {
    CLIENT.with(|c| *c.borrow_mut() = Some(client));
}

pub(crate) fn with_client<T>(f: impl FnOnce(&mut SyncClient<UserCommand>) -> T) -> T {
    CLIENT.with(|c| {
        let mut borrow = c.borrow_mut();
        let client = borrow.as_mut().expect("client not initialized — call init() first");
        f(client)
    })
}

pub(crate) fn make_db() -> Database {
    let mut db = Database::new();
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
    db
}
