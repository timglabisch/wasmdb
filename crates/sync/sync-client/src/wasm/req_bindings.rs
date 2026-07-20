//! `#[wasm_bindgen]` surface for the `requirements` runtime store.
//!
//! Mirrors the existing reactive-DB subscribe API but tracks loading
//! state for typed data dependencies. Each subscriber holds one
//! `RequirementKey` (the Derived created from `(sql, requires)`) and
//! one JS callback; `on_changed` from the dispatcher fans state changes
//! out to those callbacks.
//!
//! These four exports are non-generic over the app command type — the
//! requirements store is only keyed by SQL+args and doesn't see `C` —
//! so they live as direct `#[wasm_bindgen]` items in this library crate
//! and are picked up automatically by the downstream cdylib build.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use database_reactive::ProjectionEvent;
use requirements::{
    make_projected_key, FetchError, RequirementKey, RequirementRegistry, RequirementStore,
    SlotState, SubscriberId,
};
use serde::{Deserialize, Serialize};
use sql_engine::storage::ZSet;
use sql_parser::ast::Value;
use wasm_bindgen::prelude::*;

use crate::wasm::req_dispatcher::{OnChanged, WasmDispatcher};

struct ReqState {
    store: Rc<RefCell<RequirementStore>>,
    dispatcher: Rc<RefCell<WasmDispatcher>>,
    subscribers: Rc<RefCell<HashMap<u64, SubscriberEntry>>>,
    key_index: Rc<RefCell<HashMap<RequirementKey, Vec<SubscriberId>>>>,
    on_changed: OnChanged,
}

struct SubscriberEntry {
    key: RequirementKey,
    callback: js_sys::Function,
}

thread_local! {
    static REQ: RefCell<Option<ReqState>> = const { RefCell::new(None) };
    /// Pull source for projection failure/recovery events — a closure over
    /// the typed client (`take_projection_events`). Registered by
    /// `define_wasm_api!`'s `init` since this module cannot name the app's
    /// command type.
    static PROJECTION_SOURCE: RefCell<Option<ProjectionSource>> = const { RefCell::new(None) };
}

type ProjectionSource = Box<dyn Fn() -> Vec<ProjectionEvent>>;

/// Register the pull source for projection event draining.
pub fn set_projection_event_source(source: ProjectionSource) {
    PROJECTION_SOURCE.with(|s| *s.borrow_mut() = Some(source));
}

/// Drain projection failure/recovery events from the client and apply
/// them IN ORDER to the matching `projected:<id>:<partition>` slots: a
/// failure pins `Error`, a recovery clears the pin. Order matters —
/// multiple derive passes can run between two drains (e.g. invert + apply
/// during reconcile) and the last event per partition must win. Changed
/// slots ping their JS callbacks once. Events for partitions without a
/// slot are dropped — nobody subscribes to them. The requirements runtime
/// is checked BEFORE pulling so events are not consumed (and lost)
/// without a runtime to route them to.
pub fn drain_projection_events() {
    let installed = REQ.with(|r| r.borrow().is_some());
    if !installed {
        return;
    }
    let Some(events) = PROJECTION_SOURCE.with(|s| s.borrow().as_ref().map(|source| source()))
    else {
        return;
    };
    if events.is_empty() {
        return;
    }
    with_state(|state| {
        let mut changed = Vec::new();
        {
            let mut store = state.store.borrow_mut();
            for event in events {
                match event {
                    ProjectionEvent::Failed(f) => {
                        let Some(partition) = f.partition else { continue };
                        let slot_key = make_projected_key(&f.projection, &partition);
                        changed.extend(store.report_projection_failure(&slot_key, f.message));
                    }
                    ProjectionEvent::Recovered { projection, partition } => {
                        let slot_key = make_projected_key(&projection, &partition);
                        changed.extend(store.clear_projection_failure(&slot_key));
                    }
                }
            }
        }
        let mut seen: HashSet<&RequirementKey> = HashSet::new();
        for key in &changed {
            if seen.insert(key) {
                (state.on_changed)(key);
            }
        }
    });
}

/// No-op `register_fn` for apps that don't use the requirements
/// pipeline. Pass this as `register_requirements = ...` in
/// [`define_wasm_api!`], or omit `register_requirements` entirely.
pub fn no_register_requirements(
    _apply: Rc<dyn Fn(&ZSet) -> Result<(), String>>,
    _registry: &mut RequirementRegistry,
) {}

/// Build the requirements runtime. The `apply_zset` callback takes a
/// `ZSet` produced by the requirements pipeline and applies it to the
/// app's reactive database — typically `with_client::<C, _>(|c|
/// c.db_mut().apply_zset(zset).map_err(|e| e.to_string()))`. The
/// `register_fn` is the codegen-emitted `register_all_requirements`
/// function from the app's tables-codegen build.rs output.
pub fn install_requirements<ApplyFn, RegisterFn>(
    apply_zset: ApplyFn,
    register_fn: RegisterFn,
) where
    ApplyFn: Fn(&ZSet) -> Result<(), String> + 'static,
    RegisterFn: FnOnce(Rc<dyn Fn(&ZSet) -> Result<(), String>>, &mut RequirementRegistry),
{
    let mut registry = RequirementRegistry::new();
    // Every requirement apply runs the derive pass (apply_zset → notify);
    // drain any resulting projection events right after so slot states
    // stay in step with the data they describe.
    let apply: Rc<dyn Fn(&ZSet) -> Result<(), String>> = Rc::new(move |zset: &ZSet| {
        let result = apply_zset(zset);
        drain_projection_events();
        result
    });
    register_fn(apply, &mut registry);

    let store = Rc::new(RefCell::new(RequirementStore::new()));
    let subscribers: Rc<RefCell<HashMap<u64, SubscriberEntry>>> =
        Rc::new(RefCell::new(HashMap::new()));
    let key_index: Rc<RefCell<HashMap<RequirementKey, Vec<SubscriberId>>>> =
        Rc::new(RefCell::new(HashMap::new()));

    let on_changed = {
        let subscribers = subscribers.clone();
        let key_index = key_index.clone();
        Rc::new(move |key: &RequirementKey| {
            let sub_ids: Vec<SubscriberId> =
                key_index.borrow().get(key).cloned().unwrap_or_default();
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
        on_changed.clone(),
    )));

    REQ.with(|r| {
        *r.borrow_mut() = Some(ReqState {
            store,
            dispatcher,
            subscribers,
            key_index,
            on_changed,
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

/// One entry of the `requires` array. Untagged: the presence of
/// `projection` selects the Projected form, `id` the Fetched form.
///
/// - Fetched: `{"id": "drafts.log_by_doc", "args": [..]}`
/// - Projected: `{"projection": "invoice_draft", "partition": "<value>",
///   "requires": [..upstream entries..]}` — `partition` in the engine's
///   canonical display form (decimal for I64, raw string, hyphenated
///   lowercase UUID). Upstream is typically the partition's log fetch.
#[derive(Deserialize)]
#[serde(untagged)]
enum RequiresEntry {
    Projected {
        projection: String,
        partition: serde_json::Value,
        #[serde(default)]
        requires: Vec<RequiresEntry>,
    },
    Fetched {
        id: String,
        #[serde(default)]
        args: Vec<serde_json::Value>,
    },
}

/// Canonical partition repr for a Projected slot from its JSON form.
/// Strings pass through (UUIDs must arrive hyphenated lowercase),
/// integers render decimal — matching `DeriveFailure::partition` / the
/// engine's display form. Non-integer numbers are rejected: engine
/// partitions are I64/Str/Uuid, so a float repr could never match a
/// reported failure and would fail silently.
fn json_partition_repr(partition: &serde_json::Value) -> Result<String, String> {
    match partition {
        serde_json::Value::String(s) => Ok(s.clone()),
        serde_json::Value::Number(n) => n
            .as_i64()
            .map(|v| v.to_string())
            .ok_or_else(|| format!("projection partition: number must be an i64, got {n}")),
        other => Err(format!("projection partition: unsupported JSON type {other:?}")),
    }
}

/// Register one `requires` entry (and, for Projected, its upstream
/// subtree) in the store; returns the entry's slot key.
fn upsert_requires_entry(
    store: &mut RequirementStore,
    entry: &RequiresEntry,
) -> Result<RequirementKey, String> {
    match entry {
        RequiresEntry::Fetched { id, args } => {
            let args = json_args_to_values(args)?;
            Ok(store.upsert_fetched(id, args))
        }
        RequiresEntry::Projected { projection, partition, requires } => {
            let mut upstream = Vec::with_capacity(requires.len());
            for r in requires {
                upstream.push(upsert_requires_entry(store, r)?);
            }
            let partition_repr = json_partition_repr(partition)?;
            Ok(store.upsert_projected(projection, &partition_repr, upstream))
        }
    }
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
            let key = upsert_requires_entry(&mut store, entry).map_err(|e| JsError::new(&e))?;
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
        FetchError::Projection(s) => format!("projection: {s}"),
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
        state
            .store
            .borrow_mut()
            .invalidate(&req_key, &mut *dispatcher);
    });
}
