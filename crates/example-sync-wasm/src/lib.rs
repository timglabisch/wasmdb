use wasm_bindgen::prelude::*;
use database::Database;
use sql_engine::schema::{ColumnSchema, DataType, TableSchema};
use sql_engine::storage::CellValue;
use sync::protocol::{CommandResponse, StreamId};
use sync_client::client::SyncClient;
use sync_client::stream::StreamAction;
use example_sync_commands::UserCommand;
use std::cell::RefCell;

thread_local! {
    static CLIENT: RefCell<Option<SyncClient<UserCommand>>> = RefCell::new(None);
}

fn with_client<T>(f: impl FnOnce(&mut SyncClient<UserCommand>) -> T) -> T {
    CLIENT.with(|c| {
        let mut borrow = c.borrow_mut();
        let client = borrow.as_mut().expect("client not initialized — call init() first");
        f(client)
    })
}

fn make_db() -> Database {
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
    db
}

#[wasm_bindgen]
pub fn init() {
    CLIENT.with(|c| {
        *c.borrow_mut() = Some(SyncClient::new(make_db()));
    });
}

#[wasm_bindgen]
pub fn create_stream() -> u64 {
    with_client(|client| client.create_stream().0)
}

/// Execute an InsertUser command on the given stream.
/// Returns the borsh-serialized CommandRequest bytes to send to the server.
#[wasm_bindgen]
pub fn insert_user(stream_id: u64, id: i64, name: &str, age: i64) -> Result<Vec<u8>, JsError> {
    let cmd = UserCommand::Insert {
        id,
        name: name.to_string(),
        age,
    };

    with_client(|client| {
        let request = client.execute(StreamId(stream_id), cmd)
            .map_err(|e| JsError::new(&e.to_string()))?;
        let bytes = borsh::to_vec(&request)
            .map_err(|e| JsError::new(&e.to_string()))?;
        Ok(bytes)
    })
}

/// Feed a server response (borsh bytes) back to the client.
/// Returns a JSON string describing the action taken.
#[wasm_bindgen]
pub fn receive_response(response_bytes: &[u8]) -> Result<String, JsError> {
    let response: CommandResponse = borsh::from_slice(response_bytes)
        .map_err(|e| JsError::new(&format!("deserialize response: {e}")))?;

    with_client(|client| {
        let action = client.receive_response(response)
            .map_err(|e| JsError::new(&e.to_string()))?;

        let result = match action {
            StreamAction::Idle => "idle".to_string(),
            StreamAction::WaitingForResponse => "waiting".to_string(),
            StreamAction::AllConfirmed { confirmed_zsets } => {
                format!("confirmed:{}", confirmed_zsets.len())
            }
            StreamAction::Rejected { reason, .. } => {
                format!("rejected:{reason}")
            }
        };
        Ok(result)
    })
}

/// Query the local (optimistic) database. Returns a JSON array of users.
#[wasm_bindgen]
pub fn query_users() -> Result<String, JsError> {
    with_client(|client| {
        let result = client.db_mut()
            .execute("SELECT users.id, users.name, users.age FROM users")
            .map_err(|e| JsError::new(&e.to_string()))?;

        if result.is_empty() || result[0].is_empty() {
            return Ok("[]".to_string());
        }

        let num_rows = result[0].len();
        let mut json = String::from("[");
        for row in 0..num_rows {
            if row > 0 { json.push(','); }
            let id = match &result[0][row] { CellValue::I64(v) => *v, _ => 0 };
            let name = match &result[1][row] { CellValue::Str(s) => s.as_str(), _ => "" };
            let age = match &result[2][row] { CellValue::I64(v) => *v, _ => 0 };
            json.push_str(&format!(r#"{{"id":{id},"name":"{name}","age":{age}}}"#));
        }
        json.push(']');
        Ok(json)
    })
}
