use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use database_reactive::SubscriptionHandle;
use sync_demo_commands::UserCommand;
use sql_engine::storage::CellValue;
use sync::protocol::StreamId;
use sync_client::client::SyncClient;
use wasm_bindgen::prelude::*;

use crate::debug::{bump_notification_count, log_event, now_ms, record_query, track_table_invalidations, DebugEvent};
use crate::state::{install_client, make_db, with_client, DEFAULT_STREAM_ID};
use crate::stream::{do_flush_stream, try_drain_queue, PendingFetch, StreamHandle, STREAM_HANDLES};

#[wasm_bindgen]
pub fn init() {
    let mut client = SyncClient::new(make_db());
    let stream_id_val = client.create_stream().0;
    install_client(client);
    STREAM_HANDLES.with(|sh| {
        sh.borrow_mut().insert(stream_id_val, StreamHandle::new(1, 0, 0));
    });
    DEFAULT_STREAM_ID.with(|d| *d.borrow_mut() = Some(stream_id_val));
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
///
/// Returns `{ handle, subId }`:
/// - `handle` is per-caller and is passed back to [`unsubscribe`]. Each
///   `subscribe` call gets a fresh handle.
/// - `subId` is the shared runtime id. Multiple callers subscribing with the
///   same SQL see the same `subId` — JS stores can key by it to avoid
///   duplicating per-query state.
///
/// Subscriptions produce dirty notifications via [`on_dirty`] + [`next_dirty`].
/// No callback is registered here — dispatching to per-sub JS handlers is the
/// JS host's job so it can defer work to `requestIdleCallback` etc.
#[wasm_bindgen]
pub fn subscribe(sql: &str) -> Result<JsValue, JsError> {
    let (handle, sub_id, tables) = with_client(|client| {
        let (handle, sub_id) = client.db_mut().subscribe(sql)
            .unwrap_or_else(|e| panic!("subscribe: {e}"));
        let tables: Vec<String> = client
            .db()
            .registry()
            .table_subscriptions()
            .iter()
            .filter(|(_, subs)| subs.contains(&sub_id))
            .map(|(t, _)| t.clone())
            .collect();
        (handle, sub_id, tables)
    });

    log_event(DebugEvent::SubscriptionCreated {
        timestamp_ms: now_ms(),
        sub_id: sub_id.0,
        sql: sql.to_string(),
        tables,
    });

    let result = js_sys::Object::new();
    js_sys::Reflect::set(&result, &"handle".into(), &JsValue::from_f64(handle.0 as f64))
        .map_err(|e| JsError::new(&format!("{e:?}")))?;
    js_sys::Reflect::set(&result, &"subId".into(), &JsValue::from_f64(sub_id.0 as f64))
        .map_err(|e| JsError::new(&format!("{e:?}")))?;
    Ok(result.into())
}

/// Remove a reactive query subscription by the per-caller handle.
///
/// Unknown or already-released handles are a no-op — a warning is logged to
/// the JS console. Other callers sharing the same underlying subscription
/// remain active; the subscription is only torn down when the last handle
/// referencing it is released.
#[wasm_bindgen]
pub fn unsubscribe(handle: f64) {
    let h = SubscriptionHandle(handle as u64);
    let released = with_client(|client| client.db_mut().unsubscribe(h));
    if !released {
        web_sys::console::warn_1(
            &format!("wasmdb: unsubscribe on unknown or already-released handle {}", h.0).into(),
        );
        return;
    }
    log_event(DebugEvent::SubscriptionRemoved {
        timestamp_ms: now_ms(),
        sub_id: h.0,
    });
}

/// Register the edge-triggered wake signal. Fires once when the internal
/// dirty-set transitions from empty to non-empty. The JS host is expected to
/// schedule a drain (e.g. `queueMicrotask(drain)` or `requestIdleCallback`)
/// that pulls [`next_dirty`] in a loop until it returns null.
///
/// Only one wake function is supported; subsequent calls replace it.
#[wasm_bindgen]
pub fn on_dirty(wake: js_sys::Function) {
    with_client(|client| {
        client.db_mut().on_dirty(Box::new(move || {
            if let Err(e) = wake.call0(&JsValue::NULL) {
                web_sys::console::error_2(&"wasmdb: on_dirty wake call failed".into(), &e);
            }
        }));
    });
}

/// Pull the next dirty notification, or `null` when the drain cycle is empty.
/// Returns `{ subId, triggered: number[] }`.
///
/// The JS host loops over this until it returns null to finish a drain cycle.
/// Marks arriving between calls (or during the drain) land in the *next*
/// cycle — never dropped, never double-surfaced.
#[wasm_bindgen]
pub fn next_dirty() -> Result<JsValue, JsError> {
    with_client(|client| {
        match client.db_mut().next_dirty() {
            Some(n) => {
                bump_notification_count(n.sub_id.0);
                log_event(DebugEvent::Notification {
                    timestamp_ms: now_ms(),
                    sub_id: n.sub_id.0,
                    triggered_count: n.triggered.len(),
                });
                let obj = js_sys::Object::new();
                js_sys::Reflect::set(&obj, &"subId".into(), &JsValue::from_f64(n.sub_id.0 as f64))
                    .map_err(|e| JsError::new(&format!("{e:?}")))?;
                let arr = js_sys::Array::new_with_length(n.triggered.len() as u32);
                for (i, t) in n.triggered.iter().enumerate() {
                    arr.set(i as u32, JsValue::from_f64(*t as f64));
                }
                js_sys::Reflect::set(&obj, &"triggered".into(), &arr)
                    .map_err(|e| JsError::new(&format!("{e:?}")))?;
                Ok(obj.into())
            }
            None => Ok(JsValue::NULL),
        }
    })
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
            .execute_traced(sql)
            .map_err(|e| JsError::new(&e.to_string()))?;
        let rows = columns_to_rows(columns);
        record_query(sql, "optimistic", spans, rows.len());
        serde_wasm_bindgen::to_value(&rows).map_err(|e| JsError::new(&e.to_string()))
    })
}

/// Execute a SQL query against the confirmed (server-acknowledged) database.
/// `triggered` is a `number[]` of condition indices (typically the `triggered`
/// field from a `next_dirty` notification) used to light up `REACTIVE(...)`
/// columns. Pass an empty array (or nothing) for a cold read.
#[wasm_bindgen]
pub fn query_confirmed(sql: &str, triggered: Option<Vec<u32>>) -> Result<JsValue, JsError> {
    let triggered_set: Option<HashSet<usize>> = triggered
        .filter(|t| !t.is_empty())
        .map(|t| t.into_iter().map(|i| i as usize).collect());

    with_client(|client| {
        let (columns, spans) = client
            .confirmed_db_mut()
            .execute_traced_with_triggered(sql, triggered_set)
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
