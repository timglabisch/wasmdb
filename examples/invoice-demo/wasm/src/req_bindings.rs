//! `#[wasm_bindgen]` surface for the `requirements` runtime store.
//!
//! Mirrors the existing reactive-DB subscribe API but tracks loading
//! state for typed data dependencies. Each subscriber holds one
//! `RequirementKey` (the Derived created from `(sql, requires)`) and
//! one JS callback; `on_changed` from the dispatcher fans state changes
//! out to those callbacks.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use invoice_demo_tables_client_generated::register_all_requirements;
use requirements::{
    FetchError, RequirementKey, RequirementRegistry, RequirementStore, SlotState, SubscriberId,
};
use sql_engine::storage::ZSet;
use sql_parser::ast::Value;
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

use crate::req_dispatcher::WasmDispatcher;
use crate::state::with_client;

struct ReqState {
    store: Rc<RefCell<RequirementStore>>,
    dispatcher: Rc<RefCell<WasmDispatcher>>,
    subscribers: Rc<RefCell<HashMap<u64, SubscriberEntry>>>,
    key_index: Rc<RefCell<HashMap<RequirementKey, Vec<SubscriberId>>>>,
}

struct SubscriberEntry {
    key: RequirementKey,
    callback: js_sys::Function,
}

thread_local! {
    static REQ: RefCell<Option<ReqState>> = const { RefCell::new(None) };
}

pub fn install_requirements() {
    let mut registry = RequirementRegistry::new();
    let apply: Rc<dyn Fn(&ZSet) -> Result<(), String>> = Rc::new(|zset: &ZSet| {
        with_client(|client| {
            client
                .db_mut()
                .apply_zset(zset)
                .map_err(|e| e.to_string())
        })
    });
    register_all_requirements(apply, &mut registry);

    let store = Rc::new(RefCell::new(RequirementStore::new()));
    let subscribers: Rc<RefCell<HashMap<u64, SubscriberEntry>>> =
        Rc::new(RefCell::new(HashMap::new()));
    let key_index: Rc<RefCell<HashMap<RequirementKey, Vec<SubscriberId>>>> =
        Rc::new(RefCell::new(HashMap::new()));

    let on_changed = {
        let subscribers = subscribers.clone();
        let key_index = key_index.clone();
        Rc::new(move |key: &RequirementKey| {
            let sub_ids: Vec<SubscriberId> = key_index
                .borrow()
                .get(key)
                .cloned()
                .unwrap_or_default();
            for sid in sub_ids {
                let cb = subscribers
                    .borrow()
                    .get(&sid.0)
                    .map(|e| e.callback.clone());
                if let Some(cb) = cb {
                    if let Err(err) = cb.call0(&JsValue::NULL) {
                        web_sys::console::error_2(
                            &"requirements: callback failed".into(),
                            &err,
                        );
                    }
                }
            }
        }) as Rc<dyn Fn(&RequirementKey)>
    };

    let dispatcher = Rc::new(RefCell::new(WasmDispatcher::new(
        Rc::new(registry),
        store.clone(),
        on_changed,
    )));

    REQ.with(|r| {
        *r.borrow_mut() = Some(ReqState {
            store,
            dispatcher,
            subscribers,
            key_index,
        });
    });
}

fn with_state<T>(f: impl FnOnce(&ReqState) -> T) -> T {
    REQ.with(|r| {
        let borrow = r.borrow();
        let state = borrow
            .as_ref()
            .expect("requirements not initialized — call init() first");
        f(state)
    })
}

#[derive(Deserialize)]
struct RequiresEntry {
    id: String,
    #[serde(default)]
    args: Vec<serde_json::Value>,
}

fn json_args_to_values(args: &[serde_json::Value]) -> Result<Vec<Value>, String> {
    args.iter()
        .enumerate()
        .map(|(idx, v)| match v {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::Bool(b) => Ok(Value::Bool(*b)),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(Value::Int(i))
                } else if let Some(f) = n.as_f64() {
                    Ok(Value::Float(f))
                } else {
                    Err(format!("arg {idx}: number out of range"))
                }
            }
            serde_json::Value::String(s) => Ok(Value::Text(s.clone())),
            other => Err(format!("arg {idx}: unsupported JSON type {other:?}")),
        })
        .collect()
}

#[wasm_bindgen]
pub fn requirements_subscribe(
    sql: String,
    requires_json: String,
    on_change: js_sys::Function,
) -> Result<f64, JsError> {
    let entries: Vec<RequiresEntry> =
        serde_json::from_str(&requires_json).map_err(|e| JsError::new(&e.to_string()))?;

    with_state(|state| {
        let mut store = state.store.borrow_mut();
        let mut dispatcher = state.dispatcher.borrow_mut();

        let mut upstream = Vec::with_capacity(entries.len());
        for entry in &entries {
            let args = json_args_to_values(&entry.args).map_err(|e| JsError::new(&e))?;
            let key = store.upsert_fetched(&entry.id, args);
            upstream.push(key);
        }
        let derived_key = store.upsert_derived(
            sql.clone().into(),
            HashMap::new(),
            upstream,
            None,
        );
        let sub_id = store.subscribe(&derived_key, &mut *dispatcher);

        state
            .key_index
            .borrow_mut()
            .entry(derived_key.clone())
            .or_default()
            .push(sub_id);
        state.subscribers.borrow_mut().insert(
            sub_id.0,
            SubscriberEntry {
                key: derived_key,
                callback: on_change,
            },
        );
        Ok::<_, JsError>(sub_id.0 as f64)
    })
}

#[wasm_bindgen]
pub fn requirements_unsubscribe(sub: f64) {
    let sid = SubscriberId(sub as u64);
    with_state(|state| {
        let entry = state.subscribers.borrow_mut().remove(&sid.0);
        if let Some(entry) = entry {
            let mut idx = state.key_index.borrow_mut();
            if let Some(subs) = idx.get_mut(&entry.key) {
                subs.retain(|s| *s != sid);
                if subs.is_empty() {
                    idx.remove(&entry.key);
                }
            }
        }
        let _ = state.store.borrow_mut().unsubscribe(sid);
    });
}

#[derive(Serialize)]
struct StatusPayload {
    state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

fn slot_state_str(state: SlotState) -> &'static str {
    match state {
        SlotState::Idle => "idle",
        SlotState::Loading => "loading",
        SlotState::Ready => "ready",
        SlotState::Error => "error",
    }
}

fn fetch_error_str(err: &FetchError) -> String {
    match err {
        FetchError::Network(s) => format!("network: {s}"),
        FetchError::Server { status, body } => format!("server {status}: {body}"),
        FetchError::Decode(s) => format!("decode: {s}"),
        FetchError::Cancelled => "cancelled".into(),
    }
}

#[wasm_bindgen]
pub fn requirements_status(sub: f64) -> Result<JsValue, JsError> {
    let sid = SubscriberId(sub as u64);
    with_state(|state| {
        let key = state
            .subscribers
            .borrow()
            .get(&sid.0)
            .map(|e| e.key.clone());
        let key = key.ok_or_else(|| JsError::new("requirements_status: unknown subscriber"))?;
        let store = state.store.borrow();
        let slot = store
            .get(&key)
            .ok_or_else(|| JsError::new("requirements_status: slot missing"))?;
        let payload = StatusPayload {
            state: slot_state_str(slot.state),
            error: slot.last_error.as_ref().map(fetch_error_str),
        };
        serde_wasm_bindgen::to_value(&payload).map_err(|e| JsError::new(&e.to_string()))
    })
}

#[wasm_bindgen]
pub fn requirements_invalidate(key: String) {
    with_state(|state| {
        let mut dispatcher = state.dispatcher.borrow_mut();
        let req_key = RequirementKey::new(key);
        state.store.borrow_mut().invalidate(&req_key, &mut *dispatcher);
    });
}
