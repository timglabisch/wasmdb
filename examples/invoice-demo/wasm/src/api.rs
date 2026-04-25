use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;
#[cfg(target_arch = "wasm32")]
use std::sync::Arc;

use database_reactive::SubscriptionHandle;
use invoice_demo_commands::InvoiceCommand;
#[cfg(target_arch = "wasm32")]
use invoice_demo_tables_client_generated::{
    activity_log, contacts, customers, invoices, payments, positions, products,
    recurring_invoices, recurring_positions, sepa_mandates,
};
use sql_engine::execute::{ParamValue, Params};
use sql_engine::storage::CellValue;
use sync::protocol::StreamId;
use sync_client::client::SyncClient;
use wasm_bindgen::prelude::*;

use crate::debug::{bump_notification_count, log_event, now_ms, record_query, track_table_invalidations, DebugEvent};
use crate::state::{install_client, make_db, with_client, DEFAULT_STREAM_ID, ID_COUNTER};
use crate::stream::{do_flush_stream, try_drain_queue, PendingFetch, StreamHandle, STREAM_HANDLES};

#[wasm_bindgen]
pub fn init() {
    #[allow(unused_mut)]
    let mut db = make_db();
    // `DbCaller` impls on generated markers are `#[cfg(target_arch = "wasm32")]`
    // because the HTTP fetcher uses `JsFuture` (`!Send`); gate registration
    // the same way so native `cargo check` stays green.
    #[cfg(target_arch = "wasm32")]
    {
        db.register_caller_of::<customers::All>(Arc::new(()));
        db.register_caller_of::<contacts::All>(Arc::new(()));
        db.register_caller_of::<invoices::All>(Arc::new(()));
        db.register_caller_of::<positions::All>(Arc::new(()));
        db.register_caller_of::<payments::All>(Arc::new(()));
        db.register_caller_of::<products::All>(Arc::new(()));
        db.register_caller_of::<recurring_invoices::All>(Arc::new(()));
        db.register_caller_of::<recurring_positions::All>(Arc::new(()));
        db.register_caller_of::<sepa_mandates::All>(Arc::new(()));
        db.register_caller_of::<activity_log::All>(Arc::new(()));
    }
    let mut client = SyncClient::new(db);
    let stream_id_val = client.create_stream().0;
    install_client(client);
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

#[wasm_bindgen]
pub fn subscribe(sql: &str) -> Result<JsValue, JsError> {
    let result = with_client(|client| {
        let (handle, sub_id) = client.db_mut().subscribe(sql)
            .map_err(|e| format!("subscribe: {e} (sql={sql})"))?;
        let tables: Vec<String> = client
            .db()
            .registry()
            .table_subscriptions()
            .iter()
            .filter(|(_, subs)| subs.contains(&sub_id))
            .map(|(t, _)| t.clone())
            .collect();
        Ok::<_, String>((handle, sub_id, tables))
    });
    let (handle, sub_id, tables) = match result {
        Ok(r) => r,
        Err(msg) => return Err(JsError::new(&msg)),
    };

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

#[wasm_bindgen]
pub fn execute_on_stream(stream_id: f64, cmd_json: &str) -> Result<JsValue, JsError> {
    let stream_id_val = stream_id as u64;
    let cmd: InvoiceCommand =
        serde_json::from_str(cmd_json).map_err(|e| JsError::new(&e.to_string()))?;

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

#[wasm_bindgen]
pub fn execute(cmd_json: &str) -> Result<JsValue, JsError> {
    let stream_id = DEFAULT_STREAM_ID.with(|d| d.borrow().expect("init() not called"));
    execute_on_stream(stream_id as f64, cmd_json)
}

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

#[wasm_bindgen]
pub fn query(sql: &str, params: JsValue) -> Result<JsValue, JsError> {
    let params = js_to_params(params)?;
    with_client(|client| {
        let (columns, spans) = client
            .db_mut()
            .execute_traced_with_params(sql, params)
            .map_err(|e| JsError::new(&e.to_string()))?;
        let rows = columns_to_rows(columns);
        record_query(sql, "optimistic", spans, rows.len());
        serde_wasm_bindgen::to_value(&rows).map_err(|e| JsError::new(&e.to_string()))
    })
}

/// Async sibling of [`query`] — use whenever the SQL may contain a
/// `schema.fn(args)` source, which triggers an HTTP roundtrip to
/// `/table-fetch` during Phase 0. Plain SELECTs also work but pay the
/// async overhead, so the JS client reserves this for the fetcher path.
///
/// Fetch/execute-split: `with_client` holds the client only during the
/// sync prepare (phase 0a) and the sync apply+execute (phases 0c+1). The
/// HTTP roundtrip in between runs without any client borrow — parallel
/// `query_async` calls overlap their fetches and never double-borrow.
#[wasm_bindgen]
pub async fn query_async(sql: String, params: JsValue) -> Result<JsValue, JsError> {
    let params = js_to_params(params)?;

    let (prepared, fetchers) = with_client(|client| {
        let db = client.db_mut().db_mut_raw();
        let prepared = db.prepare_select(&sql, params)
            .map_err(|e| JsError::new(&e.to_string()))?;
        Ok::<_, JsError>((prepared, db.fetchers()))
    })?;

    let fetched = database::fetch_for(&prepared, &fetchers).await
        .map_err(|e| JsError::new(&e.to_string()))?;

    let columns = with_client(|client| {
        client.db_mut().db_mut_raw().apply_and_execute_select(prepared, fetched)
            .map_err(|e| JsError::new(&e.to_string()))
    })?;

    let rows = columns_to_rows(columns);
    // No per-step spans from the async path yet — record a zero-span row
    // so the debug panel still sees the query.
    record_query(&sql, "optimistic", vec![], rows.len());
    serde_wasm_bindgen::to_value(&rows).map_err(|e| JsError::new(&e.to_string()))
}

#[wasm_bindgen]
pub fn query_confirmed(
    sql: &str,
    triggered: Option<Vec<u32>>,
    params: JsValue,
) -> Result<JsValue, JsError> {
    let triggered_set: Option<HashSet<usize>> = triggered
        .filter(|t| !t.is_empty())
        .map(|t| t.into_iter().map(|i| i as usize).collect());
    let params = js_to_params(params)?;

    with_client(|client| {
        let (columns, spans) = client
            .confirmed_db_mut()
            .execute_traced_with_triggered_and_params(sql, triggered_set, params)
            .map_err(|e| JsError::new(&e.to_string()))?;
        let rows = columns_to_rows(columns);
        record_query(sql, "confirmed", spans, rows.len());
        serde_wasm_bindgen::to_value(&rows).map_err(|e| JsError::new(&e.to_string()))
    })
}

/// Convert a JS object `{ name: value, ... }` into `Params`. Accepts:
/// - `number` (integer) → `ParamValue::Int`
/// - `string` matching the canonical UUID form → `ParamValue::Uuid`
/// - any other `string` → `ParamValue::Text`
/// - `null` / `undefined` (value) → `ParamValue::Null`
/// - `number[]` → `ParamValue::IntList`
/// - `string[]` of UUIDs → `ParamValue::UuidList`; otherwise `ParamValue::TextList`
///
/// `null` or `undefined` for the whole argument yields an empty `Params`.
fn js_to_params(value: JsValue) -> Result<Params, JsError> {
    use js_sys::{Object, Reflect};

    if value.is_undefined() || value.is_null() {
        return Ok(Params::new());
    }
    let obj: &Object = value.dyn_ref::<Object>()
        .ok_or_else(|| JsError::new("params must be a plain object"))?;
    let keys = Object::keys(obj);
    let mut out = Params::new();
    for i in 0..keys.length() {
        let key_js = keys.get(i);
        let key = key_js.as_string()
            .ok_or_else(|| JsError::new("param key must be a string"))?;
        let val = Reflect::get(obj, &key_js)
            .map_err(|_| JsError::new(&format!("could not read param '{}'", key)))?;
        let pv = js_to_param_value(val, &key)?;
        out.insert(key, pv);
    }
    Ok(out)
}

fn js_to_param_value(value: JsValue, key: &str) -> Result<ParamValue, JsError> {
    use js_sys::Array;

    if value.is_null() || value.is_undefined() {
        return Ok(ParamValue::Null);
    }
    if let Some(s) = value.as_string() {
        if let Some(bytes) = sql_parser::uuid::parse_uuid(&s) {
            return Ok(ParamValue::Uuid(bytes));
        }
        return Ok(ParamValue::Text(s));
    }
    if let Some(n) = value.as_f64() {
        if !n.is_finite() || n.fract() != 0.0 {
            return Err(JsError::new(&format!(
                "param '{key}' must be an integer, got {n}"
            )));
        }
        return Ok(ParamValue::Int(n as i64));
    }
    if let Some(arr) = value.dyn_ref::<Array>() {
        let len = arr.length();
        if len == 0 {
            return Ok(ParamValue::IntList(vec![]));
        }
        let first = arr.get(0);
        if first.is_string() {
            let mut strs = Vec::with_capacity(len as usize);
            let mut all_uuid = true;
            let mut uuids = Vec::with_capacity(len as usize);
            for i in 0..len {
                let v = arr.get(i);
                let s = v.as_string().ok_or_else(|| {
                    JsError::new(&format!("param '{key}' array must be all strings"))
                })?;
                if all_uuid {
                    match sql_parser::uuid::parse_uuid(&s) {
                        Some(b) => uuids.push(b),
                        None => all_uuid = false,
                    }
                }
                strs.push(s);
            }
            if all_uuid {
                return Ok(ParamValue::UuidList(uuids));
            }
            return Ok(ParamValue::TextList(strs));
        }
        if first.as_f64().is_some() {
            let mut out = Vec::with_capacity(len as usize);
            for i in 0..len {
                let v = arr.get(i);
                let n = v.as_f64().ok_or_else(|| {
                    JsError::new(&format!("param '{key}' array must be all numbers"))
                })?;
                if !n.is_finite() || n.fract() != 0.0 {
                    return Err(JsError::new(&format!(
                        "param '{key}' list element {n} is not an integer"
                    )));
                }
                out.push(n as i64);
            }
            return Ok(ParamValue::IntList(out));
        }
        return Err(JsError::new(&format!(
            "param '{key}' array has unsupported element type (expected number or string)"
        )));
    }
    Err(JsError::new(&format!(
        "param '{key}' has unsupported type; expected int, string, null, int[] or string[]"
    )))
}

fn columns_to_rows(columns: Vec<Vec<CellValue>>) -> Vec<Vec<CellValue>> {
    if columns.is_empty() || columns[0].is_empty() {
        return vec![];
    }
    let num_rows = columns[0].len();
    (0..num_rows)
        .map(|i| columns.iter().map(|col| col[i].clone()).collect())
        .collect()
}

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
