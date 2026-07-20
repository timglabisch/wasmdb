//! `#[wasm_bindgen]` exports whose signatures don't mention the app's
//! command type, plus the JS↔engine value converters they use. The
//! generic-over-`C` exports (init, execute, execute_on_stream,
//! create_stream, flush_stream) are emitted by the
//! `define_wasm_api!` macro in the app crate.

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use database_reactive::SubscriptionHandle;
use sql_engine::execute::{ParamValue, Params};
use sql_engine::storage::{CellValue, Uuid};
use sync::protocol::{
    FetchRowsRequest, FetchRowsResponse, HeadsRequest, HeadsResponse,
};
use wasm_bindgen::prelude::*;
use wasmdb_debug::DebugEvent;

use crate::repair::{missing_parents, unknown_ids};
use crate::wasm::state::with_client_dyn;
use crate::wasm::stream::{now_ms, post_bytes};

#[wasm_bindgen]
pub fn subscribe(sql: &str) -> Result<JsValue, JsError> {
    let result = with_client_dyn(|client| {
        let (handle, sub_id) = client
            .db_mut()
            .subscribe(sql)
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

    wasmdb_debug::log_event(DebugEvent::SubscriptionCreated {
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
    let released = with_client_dyn(|client| client.db_mut().unsubscribe(h));
    if !released {
        web_sys::console::warn_1(
            &format!("wasmdb: unsubscribe on unknown or already-released handle {}", h.0).into(),
        );
        return;
    }
    wasmdb_debug::log_event(DebugEvent::SubscriptionRemoved {
        timestamp_ms: now_ms(),
        sub_id: h.0,
    });
}

#[wasm_bindgen]
pub fn on_dirty(wake: js_sys::Function) {
    with_client_dyn(|client| {
        client.db_mut().on_dirty(Box::new(move || {
            if let Err(e) = wake.call0(&JsValue::NULL) {
                web_sys::console::error_2(&"wasmdb: on_dirty wake call failed".into(), &e);
            }
        }));
    });
}

#[wasm_bindgen]
pub fn next_dirty() -> Result<JsValue, JsError> {
    with_client_dyn(|client| match client.db_mut().next_dirty() {
        Some(n) => {
            wasmdb_debug::bump_notification_count(n.sub_id.0);
            wasmdb_debug::log_event(DebugEvent::Notification {
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
    })
}

#[wasm_bindgen]
pub fn query(sql: &str, params: JsValue) -> Result<JsValue, JsError> {
    let params = js_to_params(params)?;
    with_client_dyn(|client| {
        let (columns, spans) = client
            .db_mut()
            .execute_traced_with_params(sql, params)
            .map_err(|e| JsError::new(&e.to_string()))?;
        let rows = columns_to_rows(columns);
        wasmdb_debug::record_query(now_ms(), sql, "optimistic", spans, rows.len());
        serde_wasm_bindgen::to_value(&rows).map_err(|e| JsError::new(&e.to_string()))
    })
}

/// Async sibling of [`query`]. Currently runs the same sync path; the
/// fetcher-resolution pipeline migrated into the dedicated
/// `requirements` crate, so any `schema.fn(args)` source is handled
/// outside this entry point.
#[wasm_bindgen]
pub async fn query_async(sql: String, params: JsValue) -> Result<JsValue, JsError> {
    query(&sql, params)
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

    with_client_dyn(|client| {
        let (columns, spans) = client
            .db_mut()
            .execute_traced_with_triggered_and_params(sql, triggered_set, params)
            .map_err(|e| JsError::new(&e.to_string()))?;
        let rows = columns_to_rows(columns);
        wasmdb_debug::record_query(now_ms(), sql, "confirmed", spans, rows.len());
        serde_wasm_bindgen::to_value(&rows).map_err(|e| JsError::new(&e.to_string()))
    })
}

/// Backward-refetch gap-repair (design §11.4). After a confirm, a
/// committed row may point at a `server_parent_id` this client never
/// fetched — a chain gap another writer left ahead of us on that
/// partition. This walks the gap frontier backward: it POSTs the unknown
/// parent PKs to `fetch_path` (a [`FetchRowsRequest`]), applies the
/// returned rows (which re-folds the affected projection partition at the
/// notify chokepoint), and repeats until the committed chain is contiguous
/// from ROOT. Resolves to the number of rows backfilled.
///
/// The loop is self-terminating: each round either shrinks the frontier or
/// the server returns nothing (a lossy/lying server stops it, never spins
/// it). In a production client this would run automatically at the confirm
/// chokepoint; the demo drives it explicitly after `.confirmed` so the
/// repair is observable.
#[wasm_bindgen]
pub async fn repair_chain(table: String, fetch_path: String) -> Result<JsValue, JsError> {
    let backfilled = walk_gap_to_root(&table, &fetch_path).await?;

    // Applying committed rows ran derive passes — route any projection
    // failure/recovery events to their requirement slots, as the confirm
    // path does.
    crate::wasm::req_bindings::drain_projection_events();

    Ok(JsValue::from_f64(backfilled as f64))
}

/// Bootstrap a projection-log table from the server (commit-chain v2).
///
/// The client DB lives entirely in wasm memory, so a page reload (or a
/// brand-new tab) starts empty and holds *nothing* — nothing references a
/// parent, so [`repair_chain`] alone can't rebuild anything. This seeds it:
///
/// 1. ask `heads_path` for the current chain heads (`HeadsRequest`),
/// 2. fetch the heads this client doesn't already hold by PK and apply them
///    (skipping known ids keeps a re-bootstrap idempotent — re-applying a
///    held row would double-count the fold), then
/// 3. [`walk_gap_to_root`] walks each chain backward to ROOT.
///
/// The result is the whole committed history reconstructed from the server
/// and the projections re-folded — so a reload restores state, and calling
/// this after a `/foreign-write` pulls another writer's entries in live.
/// Resolves to the number of rows backfilled.
#[wasm_bindgen]
pub async fn bootstrap(
    table: String,
    heads_path: String,
    fetch_path: String,
) -> Result<JsValue, JsError> {
    // 1. Current chain heads (the tip of every partition).
    let heads_request = HeadsRequest {
        table: table.clone(),
    };
    let heads_body = borsh::to_vec(&heads_request).map_err(|e| JsError::new(&e.to_string()))?;
    let heads_bytes = post_bytes(&heads_path, &heads_body)
        .await
        .map_err(|e| JsError::new(&format!("bootstrap heads failed: {e:?}")))?;
    let heads: HeadsResponse =
        borsh::from_slice(&heads_bytes).map_err(|e| JsError::new(&e.to_string()))?;

    let mut backfilled: usize = 0;

    // 2. Fetch + apply the heads we don't already hold.
    let fresh: Vec<Uuid> =
        with_client_dyn(|client| unknown_ids(client.db().db(), &table, &heads.ids));
    if !fresh.is_empty() {
        let request = FetchRowsRequest {
            table: table.clone(),
            ids: fresh,
        };
        let body = borsh::to_vec(&request).map_err(|e| JsError::new(&e.to_string()))?;
        let response_bytes = post_bytes(&fetch_path, &body)
            .await
            .map_err(|e| JsError::new(&format!("bootstrap fetch failed: {e:?}")))?;
        let response: FetchRowsResponse =
            borsh::from_slice(&response_bytes).map_err(|e| JsError::new(&e.to_string()))?;
        if !response.rows.is_empty() {
            backfilled += response.rows.len();
            with_client_dyn(|client| client.db_mut().apply_zset(&response.rows))
                .map_err(|e| JsError::new(&e.to_string()))?;
        }
    }

    // 3. Now the heads reference their predecessors — walk each to ROOT.
    backfilled += walk_gap_to_root(&table, &fetch_path).await?;

    crate::wasm::req_bindings::drain_projection_events();

    Ok(JsValue::from_f64(backfilled as f64))
}

/// The gap-repair loop shared by [`repair_chain`] and [`bootstrap`]: while
/// `table` holds a committed row whose `server_parent_id` is missing, fetch
/// that ancestor by PK from `fetch_path` and apply it, until the committed
/// chain is contiguous from ROOT. Returns the number of rows backfilled.
///
/// Self-terminating: each round either shrinks the frontier or the server
/// returns nothing (a lossy/lying server stops it, never spins it). Does not
/// drain projection events — the callers do, once, after the walk.
async fn walk_gap_to_root(table: &str, fetch_path: &str) -> Result<usize, JsError> {
    let mut backfilled: usize = 0;
    loop {
        let missing: Vec<Uuid> =
            with_client_dyn(|client| missing_parents(client.db().db(), table));
        if missing.is_empty() {
            break;
        }

        let request = FetchRowsRequest {
            table: table.to_string(),
            ids: missing,
        };
        let body = borsh::to_vec(&request).map_err(|e| JsError::new(&e.to_string()))?;
        let response_bytes = post_bytes(fetch_path, &body)
            .await
            .map_err(|e| JsError::new(&format!("repair fetch failed: {e:?}")))?;
        let response: FetchRowsResponse =
            borsh::from_slice(&response_bytes).map_err(|e| JsError::new(&e.to_string()))?;

        // The server holds no missing ancestor → the gap can't be closed
        // from here. Stop rather than loop on an unchanged frontier.
        if response.rows.is_empty() {
            break;
        }

        let applied = response.rows.len();
        with_client_dyn(|client| client.db_mut().apply_zset(&response.rows))
            .map_err(|e| JsError::new(&e.to_string()))?;
        backfilled += applied;
    }
    Ok(backfilled)
}

/// Activate a dynamic projection instance (design §12): materialize
/// `(id, name)` from the current local data and keep it in sync until the
/// matching [`projection_deactivate`]. `name` is the instance's compound
/// unique name as a JS array, e.g. `['account', 'carol']`. Repeated
/// activation refcounts.
#[wasm_bindgen]
pub fn projection_activate(id: String, name: JsValue) -> Result<(), JsError> {
    let name = js_to_instance_name(name)?;
    with_client_dyn(|client| client.db_mut().activate_projection(&id, name))
        .map_err(|e| JsError::new(&e.to_string()))?;
    // The activation ran a fold — surface any failure/recovery events the
    // same way the confirm/repair paths do.
    crate::wasm::req_bindings::drain_projection_events();
    Ok(())
}

/// Release one activation of `(id, name)`. The last release retracts the
/// instance's output rows.
#[wasm_bindgen]
pub fn projection_deactivate(id: String, name: JsValue) -> Result<(), JsError> {
    let name = js_to_instance_name(name)?;
    with_client_dyn(|client| client.db_mut().deactivate_projection(&id, &name))
        .map_err(|e| JsError::new(&e.to_string()))?;
    crate::wasm::req_bindings::drain_projection_events();
    Ok(())
}

/// Convert a JS array of name components into an engine instance name:
/// integer `number` → `CellValue::I64`, `string` → `CellValue::Str`.
/// Uuid components are stage 2 (an explicit form, no string sniffing).
fn js_to_instance_name(name: JsValue) -> Result<Vec<CellValue>, JsError> {
    let arr = name
        .dyn_ref::<js_sys::Array>()
        .ok_or_else(|| JsError::new("projection instance name must be an array"))?;
    let mut out = Vec::with_capacity(arr.length() as usize);
    for i in 0..arr.length() {
        let v = arr.get(i);
        if let Some(s) = v.as_string() {
            out.push(CellValue::Str(s));
            continue;
        }
        if let Some(n) = v.as_f64() {
            if !n.is_finite() || n.fract() != 0.0 {
                return Err(JsError::new(&format!(
                    "name component {i} must be an integer, got {n}"
                )));
            }
            out.push(CellValue::I64(n as i64));
            continue;
        }
        return Err(JsError::new(&format!(
            "name component {i} has unsupported type (expected string or integer)"
        )));
    }
    Ok(out)
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
pub fn js_to_params(value: JsValue) -> Result<Params, JsError> {
    use js_sys::{Object, Reflect};

    if value.is_undefined() || value.is_null() {
        return Ok(Params::new());
    }
    let obj: &Object = value
        .dyn_ref::<Object>()
        .ok_or_else(|| JsError::new("params must be a plain object"))?;
    let keys = Object::keys(obj);
    let mut out = Params::new();
    for i in 0..keys.length() {
        let key_js = keys.get(i);
        let key = key_js
            .as_string()
            .ok_or_else(|| JsError::new("param key must be a string"))?;
        let val = Reflect::get(obj, &key_js)
            .map_err(|_| JsError::new(&format!("could not read param '{}'", key)))?;
        let pv = js_to_param_value(val, &key)?;
        out.insert(key, pv);
    }
    Ok(out)
}

pub fn js_to_param_value(value: JsValue, key: &str) -> Result<ParamValue, JsError> {
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

pub fn columns_to_rows(columns: Vec<Vec<CellValue>>) -> Vec<Vec<CellValue>> {
    if columns.is_empty() || columns[0].is_empty() {
        return vec![];
    }
    let num_rows = columns[0].len();
    (0..num_rows)
        .map(|i| columns.iter().map(|col| col[i].clone()).collect())
        .collect()
}

pub fn make_manual_promise() -> (js_sys::Function, js_sys::Function, js_sys::Promise) {
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
