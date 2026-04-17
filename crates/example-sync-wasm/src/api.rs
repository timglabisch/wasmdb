use std::cell::RefCell;
use std::rc::Rc;

use database_reactive::SubId;
use example_sync_commands::UserCommand;
use sql_engine::storage::CellValue;
use sync::protocol::StreamId;
use sync_client::client::SyncClient;
use wasm_bindgen::prelude::*;

use crate::debug::{log_event, now_ms, record_query, track_table_invalidations, DebugEvent};
use crate::reactive::wrap_js_callback;
use crate::state::{make_db, with_client, DEFAULT_STREAM_ID, ID_COUNTER};
use crate::stream::{do_flush_stream, try_drain_queue, PendingFetch, StreamHandle, STREAM_HANDLES};

#[wasm_bindgen]
pub fn init() {
    let mut client = SyncClient::new(make_db());
    let stream_id = client.create_stream();
    let stream_id_val = stream_id.0;

    crate::state::CLIENT.with(|c| *c.borrow_mut() = Some(client));
    STREAM_HANDLES.with(|sh| {
        sh.borrow_mut().insert(stream_id_val, StreamHandle::new(1, 0, 0));
    });
    DEFAULT_STREAM_ID.with(|d| *d.borrow_mut() = Some(stream_id_val));
}

#[wasm_bindgen]
pub fn next_id() -> f64 {
    ID_COUNTER.with(|c| {
        let mut val = c.borrow_mut();
        *val += 1;
        *val as f64
    })
}

/// Create a new stream with batching configuration. `batch_count=1` means sequential.
#[wasm_bindgen]
pub fn create_stream(batch_count: u32, batch_wait_ms: u32, retry_count: u32) -> f64 {
    with_client(|client| {
        let stream_id = client.create_stream();
        let stream_id_val = stream_id.0;
        STREAM_HANDLES.with(|sh| {
            sh.borrow_mut().insert(
                stream_id_val,
                StreamHandle::new(batch_count as usize, batch_wait_ms, retry_count),
            );
        });
        stream_id_val as f64
    })
}

/// Register a reactive query subscription on the optimistic database.
#[wasm_bindgen]
pub fn subscribe(sql: &str, callback: js_sys::Function) -> f64 {
    let cb = wrap_js_callback(callback);
    let (sub_id, tables) = with_client(|client| {
        let sub_id = client.subscribe(sql, cb)
            .unwrap_or_else(|e| panic!("subscribe: {e}"));
        let tables: Vec<String> = client
            .db()
            .registry()
            .table_subscriptions()
            .iter()
            .filter(|(_, subs)| subs.contains(&sub_id))
            .map(|(t, _)| t.clone())
            .collect();
        (sub_id, tables)
    });

    log_event(DebugEvent::SubscriptionCreated {
        timestamp_ms: now_ms(),
        sub_id: sub_id.0,
        sql: sql.to_string(),
        tables,
    });
    sub_id.0 as f64
}

/// Remove a reactive query subscription.
#[wasm_bindgen]
pub fn unsubscribe(sub_id: f64) {
    let id = SubId(sub_id as u64);
    with_client(|client| client.unsubscribe(id));
    log_event(DebugEvent::SubscriptionRemoved {
        timestamp_ms: now_ms(),
        sub_id: sub_id as u64,
    });
}

/// Execute a command on a specific stream. Returns `{ zset, confirmed: Promise }`.
#[wasm_bindgen]
pub fn execute_on_stream(stream_id: f64, cmd_json: &str) -> Result<JsValue, JsError> {
    let stream_id_val = stream_id as u64;
    let cmd: UserCommand =
        serde_json::from_str(cmd_json).map_err(|e| JsError::new(&e.to_string()))?;

    // Optimistic execute — reactive notify happens inside `client.execute`.
    let request = with_client(|client| {
        client.execute(StreamId(stream_id_val), cmd)
            .map_err(|e| JsError::new(&e.to_string()))
    })?;

    log_event(DebugEvent::Execute {
        timestamp_ms: now_ms(),
        stream_id: stream_id_val,
        command_json: cmd_json.to_string(),
        zset_entry_count: request.client_zset.len(),
    });

    let zset_js = serde_wasm_bindgen::to_value(&request.client_zset)
        .map_err(|e| JsError::new(&e.to_string()))?;

    track_table_invalidations(&request.client_zset);

    let (resolve, reject, confirmed) = make_manual_promise();

    STREAM_HANDLES.with(|sh| {
        let mut sh = sh.borrow_mut();
        let handle = sh.get_mut(&stream_id_val)
            .expect("unknown stream — call create_stream() first");
        handle.queue.push(PendingFetch { request, resolve, reject });
    });

    try_drain_queue(stream_id_val);

    let result = js_sys::Object::new();
    js_sys::Reflect::set(&result, &"zset".into(), &zset_js)
        .map_err(|e| JsError::new(&format!("{e:?}")))?;
    js_sys::Reflect::set(&result, &"confirmed".into(), &confirmed)
        .map_err(|e| JsError::new(&format!("{e:?}")))?;
    Ok(result.into())
}

/// Execute a command on the default stream (batch_count=1, sequential).
#[wasm_bindgen]
pub fn execute(cmd_json: &str) -> Result<JsValue, JsError> {
    let stream_id = DEFAULT_STREAM_ID.with(|d| d.borrow().expect("init() not called"));
    execute_on_stream(stream_id as f64, cmd_json)
}

/// Flush all queued commands on a stream. Resolves when all are confirmed or rejected.
#[wasm_bindgen]
pub fn flush_stream(stream_id: f64) -> js_sys::Promise {
    let stream_id_val = stream_id as u64;

    let is_done = STREAM_HANDLES.with(|sh| {
        let sh = sh.borrow();
        sh.get(&stream_id_val)
            .map_or(true, |h| h.queue.is_empty() && !h.in_flight)
    });

    if is_done {
        return js_sys::Promise::resolve(&JsValue::UNDEFINED);
    }

    let (resolve, _reject, promise) = make_manual_promise();

    STREAM_HANDLES.with(|sh| {
        if let Some(handle) = sh.borrow_mut().get_mut(&stream_id_val) {
            handle.flush_waiters.push(resolve);
        }
    });

    let not_in_flight = STREAM_HANDLES.with(|sh| {
        sh.borrow().get(&stream_id_val).map_or(true, |h| !h.in_flight)
    });
    if not_in_flight {
        do_flush_stream(stream_id_val, true);
    }

    promise
}

/// Execute a SQL query against the optimistic (local) database.
#[wasm_bindgen]
pub fn query(sql: &str) -> Result<JsValue, JsError> {
    with_client(|client| {
        let (columns, spans) = client
            .db_mut()
            .execute_for_sql(sql)
            .map_err(|e| JsError::new(&e.to_string()))?;
        let rows = columns_to_rows(columns);
        record_query(sql, "optimistic", spans, rows.len());
        serde_wasm_bindgen::to_value(&rows).map_err(|e| JsError::new(&e.to_string()))
    })
}

/// Execute a SQL query against the confirmed (server-acknowledged) database.
/// The triggered-conditions set is pulled from the reactive optimistic side so
/// `REACTIVE(...)` columns still reflect the last fire.
#[wasm_bindgen]
pub fn query_confirmed(sql: &str) -> Result<JsValue, JsError> {
    with_client(|client| {
        let triggered = client.db_mut().take_triggered_for_sql(sql);
        let (columns, spans) = client
            .confirmed_db_mut()
            .execute_traced_with_triggered(sql, triggered)
            .map_err(|e| JsError::new(&e.to_string()))?;
        let rows = columns_to_rows(columns);
        record_query(sql, "confirmed", spans, rows.len());
        serde_wasm_bindgen::to_value(&rows).map_err(|e| JsError::new(&e.to_string()))
    })
}

// ── Helpers ─────────────────────────────────────────────────────────

fn columns_to_rows(columns: Vec<Vec<CellValue>>) -> Vec<Vec<CellValue>> {
    if columns.is_empty() || columns[0].is_empty() {
        return vec![];
    }
    let num_rows = columns[0].len();
    (0..num_rows)
        .map(|i| columns.iter().map(|col| col[i].clone()).collect())
        .collect()
}

/// Build a Promise with externally-accessible resolve/reject functions.
fn make_manual_promise() -> (js_sys::Function, js_sys::Function, js_sys::Promise) {
    let resolve_slot: Rc<RefCell<Option<js_sys::Function>>> = Rc::new(RefCell::new(None));
    let reject_slot: Rc<RefCell<Option<js_sys::Function>>> = Rc::new(RefCell::new(None));
    let rs = resolve_slot.clone();
    let rj = reject_slot.clone();
    let promise = js_sys::Promise::new(&mut move |resolve, reject| {
        *rs.borrow_mut() = Some(resolve);
        *rj.borrow_mut() = Some(reject);
    });
    let resolve = resolve_slot.borrow_mut().take().unwrap();
    let reject = reject_slot.borrow_mut().take().unwrap();
    (resolve, reject, promise)
}
