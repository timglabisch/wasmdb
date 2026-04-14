use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use js_sys::Uint8Array;
use database::Database;
use sql_engine::schema::{ColumnSchema, DataType, TableSchema};
use sql_engine::storage::CellValue;
use sync::protocol::CommandResponse;
use sync_client::client::SyncClient;
use sync_client::stream::StreamAction;
use example_sync_commands::UserCommand;
use std::cell::RefCell;
use std::collections::HashSet;

thread_local! {
    static CLIENT: RefCell<Option<SyncClient<UserCommand>>> = RefCell::new(None);
    static ON_CHANGE: RefCell<Option<js_sys::Function>> = RefCell::new(None);
    static ID_COUNTER: RefCell<i64> = RefCell::new(0);
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

fn notify_change() {
    ON_CHANGE.with(|cb| {
        if let Some(f) = cb.borrow().as_ref() {
            let _ = f.call0(&JsValue::NULL);
        }
    });
}

// ── Internal: fetch via web-sys ──────────────────────────────────

async fn do_fetch(body: &[u8]) -> Result<Vec<u8>, JsValue> {
    let opts = web_sys::RequestInit::new();
    opts.set_method("POST");
    let uint8_body = Uint8Array::from(body);
    opts.set_body(&uint8_body);

    let request = web_sys::Request::new_with_str_and_init("/command", &opts)?;
    request.headers().set("Content-Type", "application/octet-stream")?;

    let window = web_sys::window().ok_or_else(|| JsValue::from_str("no global window"))?;
    let resp_value = JsFuture::from(window.fetch_with_request(&request)).await?;
    let resp: web_sys::Response = resp_value.dyn_into()?;

    if !resp.ok() {
        let text = JsFuture::from(resp.text()?).await?;
        return Err(JsValue::from_str(&format!(
            "HTTP {}: {}",
            resp.status(),
            text.as_string().unwrap_or_default()
        )));
    }

    let buf = JsFuture::from(resp.array_buffer()?).await?;
    let uint8 = Uint8Array::new(&buf);
    Ok(uint8.to_vec())
}

// ── Internal: query helpers ──────────────────────────────────────

fn query_user_rows(db: &mut Database) -> Result<Vec<(i64, String, i64)>, JsError> {
    let result = db
        .execute("SELECT users.id, users.name, users.age FROM users")
        .map_err(|e| JsError::new(&e.to_string()))?;

    if result.is_empty() || result[0].is_empty() {
        return Ok(vec![]);
    }

    let mut rows = Vec::new();
    for i in 0..result[0].len() {
        let id = match &result[0][i] {
            CellValue::I64(v) => *v,
            _ => 0,
        };
        let name = match &result[1][i] {
            CellValue::Str(s) => s.clone(),
            _ => String::new(),
        };
        let age = match &result[2][i] {
            CellValue::I64(v) => *v,
            _ => 0,
        };
        rows.push((id, name, age));
    }
    Ok(rows)
}

#[derive(serde::Serialize)]
struct UserRow {
    id: i64,
    name: String,
    age: i64,
    sync: &'static str,
}

fn build_action_result(action: &StreamAction) -> Result<JsValue, JsValue> {
    let result = js_sys::Object::new();
    match action {
        StreamAction::AllConfirmed { .. } => {
            js_sys::Reflect::set(&result, &"status".into(), &"confirmed".into())?;
        }
        StreamAction::Rejected { reason, .. } => {
            js_sys::Reflect::set(&result, &"status".into(), &"rejected".into())?;
            js_sys::Reflect::set(&result, &"reason".into(), &reason.into())?;
        }
        StreamAction::WaitingForResponse => {
            js_sys::Reflect::set(&result, &"status".into(), &"waiting".into())?;
        }
        StreamAction::Idle => {
            js_sys::Reflect::set(&result, &"status".into(), &"confirmed".into())?;
        }
    }
    Ok(result.into())
}

// ── Exported API ─────────────────────────────────────────────────

#[wasm_bindgen]
pub fn init() {
    CLIENT.with(|c| {
        *c.borrow_mut() = Some(SyncClient::new(make_db()));
    });
}

#[wasm_bindgen]
pub fn set_on_change(callback: js_sys::Function) {
    ON_CHANGE.with(|cb| {
        *cb.borrow_mut() = Some(callback);
    });
}

#[wasm_bindgen]
pub fn next_id() -> f64 {
    ID_COUNTER.with(|c| {
        let mut val = c.borrow_mut();
        *val += 1;
        *val as f64
    })
}

/// Execute a command optimistically. Returns `{ zset, confirmed: Promise }`.
///
/// The ZSet is the optimistic change applied locally.
/// The Promise resolves with `{ status: "confirmed"|"rejected", reason?: string }`.
#[wasm_bindgen]
pub fn execute(cmd_json: &str) -> Result<JsValue, JsError> {
    let cmd: UserCommand =
        serde_json::from_str(cmd_json).map_err(|e| JsError::new(&e.to_string()))?;

    // Optimistically execute (synchronous)
    let request = with_client(|client| {
        let stream_id = client.create_stream();
        client
            .execute(stream_id, cmd)
            .map_err(|e| JsError::new(&e.to_string()))
    })?;

    // ZSet as JsValue via serde-wasm-bindgen
    let zset_js = serde_wasm_bindgen::to_value(&request.client_zset)
        .map_err(|e| JsError::new(&e.to_string()))?;

    // Borsh bytes for the server
    let request_bytes = borsh::to_vec(&request).map_err(|e| JsError::new(&e.to_string()))?;

    // Notify: optimistic state changed
    notify_change();

    // Build Promise for server confirmation
    let confirmed =
        wasm_bindgen_futures::future_to_promise(async move {
            let response_bytes = do_fetch(&request_bytes).await?;

            let response: CommandResponse =
                borsh::from_slice(&response_bytes).map_err(|e| {
                    JsValue::from_str(&format!("deserialize response: {e}"))
                })?;

            let action = CLIENT.with(|c| {
                let mut borrow = c.borrow_mut();
                let client = borrow
                    .as_mut()
                    .ok_or_else(|| JsValue::from_str("client not initialized"))?;
                client.receive_response(response).map_err(|e| {
                    JsValue::from_str(&e.to_string())
                })
            })?;

            // Notify: confirmed/rejected state changed
            notify_change();

            build_action_result(&action)
        });

    // Return { zset, confirmed }
    let result = js_sys::Object::new();
    js_sys::Reflect::set(&result, &"zset".into(), &zset_js)
        .map_err(|e| JsError::new(&format!("{e:?}")))?;
    js_sys::Reflect::set(&result, &"confirmed".into(), &confirmed)
        .map_err(|e| JsError::new(&format!("{e:?}")))?;

    Ok(result.into())
}

/// Query users with sync status. Returns a JS array of `{ id, name, age, sync }`.
/// `sync` is "pending" or "confirmed" — computed by diffing optimistic vs confirmed DB.
#[wasm_bindgen]
pub fn query_users() -> Result<JsValue, JsError> {
    with_client(|client| {
        let optimistic = query_user_rows(client.db_mut())?;

        let confirmed = query_user_rows(client.confirmed_db_mut())?;
        let confirmed_ids: HashSet<i64> = confirmed.iter().map(|r| r.0).collect();

        let users: Vec<UserRow> = optimistic
            .into_iter()
            .map(|(id, name, age)| UserRow {
                id,
                name,
                age,
                sync: if confirmed_ids.contains(&id) {
                    "confirmed"
                } else {
                    "pending"
                },
            })
            .collect();

        serde_wasm_bindgen::to_value(&users).map_err(|e| JsError::new(&e.to_string()))
    })
}
