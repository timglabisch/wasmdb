use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use js_sys::Uint8Array;
use database::Database;
use serde::Serialize;
use sql_engine::schema::{ColumnSchema, DataType, IndexSchema, IndexType, TableSchema};
use sql_engine::storage::{CellValue, TypedColumn};
use sql_engine::execute::Span;
use sql_engine::reactive::registry::{SubscriptionRegistry, SubId};
use sync::protocol::{BatchCommandRequest, BatchCommandResponse, Verdict};
use sync::zset::ZSet;
use sync_client::client::SyncClient;
use example_sync_commands::UserCommand;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

// ── Debug event log ────────────────────────────────────────────────

#[derive(Clone, Serialize)]
#[serde(tag = "kind")]
enum DebugEvent {
    Execute { timestamp_ms: f64, stream_id: u64, command_json: String, zset_entry_count: usize },
    FetchStart { timestamp_ms: f64, stream_id: u64, request_bytes: usize },
    FetchEnd { timestamp_ms: f64, stream_id: u64, response_bytes: usize, latency_ms: f64 },
    Confirmed { timestamp_ms: f64, stream_id: u64 },
    Rejected { timestamp_ms: f64, stream_id: u64, reason: String },
    Notification { timestamp_ms: f64, affected_sub_ids: Vec<u64>, total_subs: usize },
    SubscriptionCreated { timestamp_ms: f64, sub_id: u64, sql: String, tables: Vec<String> },
    SubscriptionRemoved { timestamp_ms: f64, sub_id: u64 },
    QueryExecuted { timestamp_ms: f64, sql: String, duration_us: u64, row_count: usize, source: String },
    SlowQuery { timestamp_ms: f64, sql: String, duration_us: u64 },
}

const EVENT_LOG_CAPACITY: usize = 512;

struct EventLog {
    events: Vec<DebugEvent>,
    write_pos: usize,
    total_count: u64,
}

impl EventLog {
    fn new() -> Self {
        Self { events: Vec::with_capacity(EVENT_LOG_CAPACITY), write_pos: 0, total_count: 0 }
    }

    fn push(&mut self, event: DebugEvent) {
        if self.events.len() < EVENT_LOG_CAPACITY {
            self.events.push(event);
        } else {
            self.events[self.write_pos] = event;
        }
        self.write_pos = (self.write_pos + 1) % EVENT_LOG_CAPACITY;
        self.total_count += 1;
    }

    fn drain_ordered(&self) -> Vec<&DebugEvent> {
        if self.events.len() < EVENT_LOG_CAPACITY {
            self.events.iter().collect()
        } else {
            let mut result = Vec::with_capacity(EVENT_LOG_CAPACITY);
            result.extend(&self.events[self.write_pos..]);
            result.extend(&self.events[..self.write_pos]);
            result
        }
    }
}

fn now_ms() -> f64 {
    js_sys::Date::now()
}

fn log_event(event: DebugEvent) {
    DEBUG_LOG.with(|log| log.borrow_mut().push(event));
}

// ── Query trace log ───────────────────────────────────────────────

#[derive(Clone, Serialize)]
struct QueryTrace {
    timestamp_ms: f64,
    sql: String,
    duration_us: u64,
    row_count: usize,
    source: String,
    spans: Vec<Span>,
    is_slow: bool,
}

const QUERY_LOG_CAPACITY: usize = 64;
const SLOW_QUERY_THRESHOLD_US: u64 = 10_000; // 10ms

struct QueryLog {
    queries: Vec<QueryTrace>,
    write_pos: usize,
    total_count: u64,
    slow_count: u64,
}

impl QueryLog {
    fn new() -> Self {
        Self { queries: Vec::with_capacity(QUERY_LOG_CAPACITY), write_pos: 0, total_count: 0, slow_count: 0 }
    }

    fn push(&mut self, trace: QueryTrace) {
        if trace.is_slow { self.slow_count += 1; }
        if self.queries.len() < QUERY_LOG_CAPACITY {
            self.queries.push(trace);
        } else {
            self.queries[self.write_pos] = trace;
        }
        self.write_pos = (self.write_pos + 1) % QUERY_LOG_CAPACITY;
        self.total_count += 1;
    }

    fn drain_ordered(&self) -> Vec<&QueryTrace> {
        if self.queries.len() < QUERY_LOG_CAPACITY {
            self.queries.iter().collect()
        } else {
            let mut result = Vec::with_capacity(QUERY_LOG_CAPACITY);
            result.extend(&self.queries[self.write_pos..]);
            result.extend(&self.queries[..self.write_pos]);
            result
        }
    }
}

// ── Stream batching ─────────────────────────────────────────────────

struct PendingFetch {
    request: sync::protocol::CommandRequest<UserCommand>,
    resolve: js_sys::Function,
    reject: js_sys::Function,
}

struct StreamHandle {
    #[allow(dead_code)]
    stream_id: sync::protocol::StreamId,
    batch_count: usize,
    batch_wait_ms: u32,
    retry_count: u32,
    queue: Vec<PendingFetch>,
    in_flight: bool,
    flush_waiters: Vec<js_sys::Function>,
    microtask_scheduled: bool,
}

// ── Thread locals ──────────────────────────────────────────────────

thread_local! {
    static CLIENT: RefCell<Option<SyncClient<UserCommand>>> = RefCell::new(None);
    static STREAM_HANDLES: RefCell<HashMap<u64, StreamHandle>> = RefCell::new(HashMap::new());
    static DEFAULT_STREAM_ID: RefCell<Option<u64>> = RefCell::new(None);
    static REGISTRY: RefCell<SubscriptionRegistry> = RefCell::new(SubscriptionRegistry::new());
    static CALLBACKS: RefCell<HashMap<u64, js_sys::Function>> = RefCell::new(HashMap::new());
    static SUB_SQL: RefCell<HashMap<u64, String>> = RefCell::new(HashMap::new());
    static ID_COUNTER: RefCell<i64> = RefCell::new(0);
    static DEBUG_LOG: RefCell<EventLog> = RefCell::new(EventLog::new());
    static NOTIFICATION_COUNTS: RefCell<HashMap<u64, u64>> = RefCell::new(HashMap::new());
    static QUERY_LOG: RefCell<QueryLog> = RefCell::new(QueryLog::new());
    static TABLE_INVALIDATION_COUNTS: RefCell<HashMap<String, u64>> = RefCell::new(HashMap::new());
    static TRIGGERED_CONDITIONS: RefCell<HashMap<u64, HashSet<usize>>> = RefCell::new(HashMap::new());
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

// ── Reactive notification ───────────────────────────────────────

/// Determine affected subscriptions from a ZSet and schedule deferred notification.
/// Also stores which reactive condition indices triggered per subscription, so that
/// SELECT reactive(...) columns can return true/false on re-execution.
fn notify_affected(zset: &ZSet) {
    let affected: HashMap<SubId, HashSet<usize>> = REGISTRY.with(|r| {
        let reg = r.borrow();
        let mut affected: HashMap<SubId, HashSet<usize>> = HashMap::new();
        for entry in &zset.entries {
            let detailed = if entry.weight > 0 {
                reg.on_insert_detailed(&entry.table, &entry.row)
            } else {
                reg.on_delete_detailed(&entry.table, &entry.row)
            };
            for (sub_id, indices) in detailed {
                affected.entry(sub_id).or_default().extend(indices);
            }
            TABLE_INVALIDATION_COUNTS.with(|tc| {
                *tc.borrow_mut().entry(entry.table.clone()).or_insert(0) += 1;
            });
        }
        affected
    });

    if affected.is_empty() {
        return;
    }

    let sub_ids: Vec<u64> = affected.keys().map(|s| s.0).collect();
    let total_subs = CALLBACKS.with(|cbs| cbs.borrow().len());
    log_event(DebugEvent::Notification {
        timestamp_ms: now_ms(),
        affected_sub_ids: sub_ids.clone(),
        total_subs,
    });
    NOTIFICATION_COUNTS.with(|nc| {
        let mut nc = nc.borrow_mut();
        for id in &sub_ids {
            *nc.entry(*id).or_insert(0) += 1;
        }
    });

    // Store triggered conditions so query re-execution can read them.
    TRIGGERED_CONDITIONS.with(|tc| {
        let mut tc = tc.borrow_mut();
        for (sub_id, indices) in &affected {
            tc.insert(sub_id.0, indices.clone());
        }
    });

    // Fire callbacks asynchronously so the click handler returns immediately.
    wasm_bindgen_futures::spawn_local(async move {
        CALLBACKS.with(|cbs| {
            let cbs = cbs.borrow();
            for sub_id in affected.keys() {
                if let Some(f) = cbs.get(&sub_id.0) {
                    if let Err(e) = f.call0(&JsValue::NULL) {
                        web_sys::console::error_2(
                            &format!("subscription {} callback error:", sub_id.0).into(),
                            &e,
                        );
                    }
                }
            }
        });
    });
}

/// Notify all subscribers (used after server response / rollback).
fn notify_all() {
    wasm_bindgen_futures::spawn_local(async {
        CALLBACKS.with(|cbs| {
            let cbs = cbs.borrow();
            NOTIFICATION_COUNTS.with(|nc| {
                let mut nc = nc.borrow_mut();
                for id in cbs.keys() {
                    *nc.entry(*id).or_insert(0) += 1;
                }
            });
            for (id, f) in cbs.iter() {
                if let Err(e) = f.call0(&JsValue::NULL) {
                    web_sys::console::error_2(
                        &format!("subscription {} callback error:", id).into(),
                        &e,
                    );
                }
            }
        });
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

/// Find the triggered condition indices for a SQL query's subscription.
/// Returns None if no subscription exists or no conditions were triggered.
fn find_triggered_for_sql(sql: &str) -> Option<HashSet<usize>> {
    SUB_SQL.with(|s| {
        let s = s.borrow();
        let sub_id = s.iter().find(|(_, v)| v.as_str() == sql).map(|(k, _)| *k)?;
        TRIGGERED_CONDITIONS.with(|tc| tc.borrow_mut().remove(&sub_id))
    })
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



// ── Stream batching logic ───────────────────────────────────────

fn try_drain_queue(stream_id_val: u64) {
    let action = STREAM_HANDLES.with(|sh| {
        let sh = sh.borrow();
        let handle = match sh.get(&stream_id_val) {
            Some(h) => h,
            None => return None,
        };

        if handle.in_flight || handle.queue.is_empty() {
            return None;
        }

        // Full batch or batch_count==1 → flush immediately
        if handle.queue.len() >= handle.batch_count || handle.batch_count == 1 {
            return Some(DrainAction::FlushNow);
        }

        // Partial batch — need to schedule delayed flush
        if !handle.microtask_scheduled {
            if handle.batch_wait_ms > 0 {
                Some(DrainAction::ScheduleTimer)
            } else {
                Some(DrainAction::ScheduleMicrotask)
            }
        } else {
            None
        }
    });

    match action {
        Some(DrainAction::FlushNow) => {
            do_flush_stream(stream_id_val, false);
        }
        Some(DrainAction::ScheduleTimer) => {
            STREAM_HANDLES.with(|sh| {
                if let Some(handle) = sh.borrow_mut().get_mut(&stream_id_val) {
                    handle.microtask_scheduled = true;
                }
            });
            let cb = wasm_bindgen::closure::Closure::once_into_js(move || {
                STREAM_HANDLES.with(|sh| {
                    if let Some(handle) = sh.borrow_mut().get_mut(&stream_id_val) {
                        handle.microtask_scheduled = false;
                    }
                });
                do_flush_stream(stream_id_val, false);
            });
            let wait_ms = STREAM_HANDLES.with(|sh| {
                sh.borrow().get(&stream_id_val).map_or(0, |h| h.batch_wait_ms)
            });
            if let Some(window) = web_sys::window() {
                let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(
                    cb.unchecked_ref(),
                    wait_ms as i32,
                );
            }
        }
        Some(DrainAction::ScheduleMicrotask) => {
            STREAM_HANDLES.with(|sh| {
                if let Some(handle) = sh.borrow_mut().get_mut(&stream_id_val) {
                    handle.microtask_scheduled = true;
                }
            });
            wasm_bindgen_futures::spawn_local(async move {
                STREAM_HANDLES.with(|sh| {
                    if let Some(handle) = sh.borrow_mut().get_mut(&stream_id_val) {
                        handle.microtask_scheduled = false;
                    }
                });
                do_flush_stream(stream_id_val, false);
            });
        }
        None => {}
    }
}

enum DrainAction {
    FlushNow,
    ScheduleTimer,
    ScheduleMicrotask,
}

fn do_flush_stream(stream_id_val: u64, take_all: bool) {
    let (items, retry_count) = STREAM_HANDLES.with(|sh| {
        let mut sh = sh.borrow_mut();
        let handle = match sh.get_mut(&stream_id_val) {
            Some(h) => h,
            None => return (Vec::new(), 0),
        };

        let count = if take_all {
            handle.queue.len()
        } else {
            handle.batch_count.min(handle.queue.len())
        };
        let items: Vec<PendingFetch> = handle.queue.drain(..count).collect();
        handle.in_flight = true;
        (items, handle.retry_count)
    });

    if items.is_empty() {
        finish_flush(stream_id_val);
        return;
    }

    // Build batch request
    let batch_request = BatchCommandRequest {
        requests: items.iter().map(|p| p.request.clone()).collect(),
    };
    let batch_bytes = match borsh::to_vec(&batch_request) {
        Ok(b) => b,
        Err(e) => {
            let err = JsValue::from_str(&format!("serialize batch: {e}"));
            for item in &items {
                let _ = item.reject.call1(&JsValue::NULL, &err);
            }
            finish_flush(stream_id_val);
            return;
        }
    };

    log_event(DebugEvent::FetchStart {
        timestamp_ms: now_ms(),
        stream_id: stream_id_val,
        request_bytes: batch_bytes.len(),
    });

    wasm_bindgen_futures::spawn_local(async move {
        let fetch_start = now_ms();

        // Retry loop
        let mut last_err: Option<JsValue> = None;
        let mut response_bytes = None;
        for _attempt in 0..=retry_count {
            match do_fetch(&batch_bytes).await {
                Ok(bytes) => {
                    response_bytes = Some(bytes);
                    break;
                }
                Err(e) => {
                    last_err = Some(e);
                }
            }
        }

        let fetch_end = now_ms();
        log_event(DebugEvent::FetchEnd {
            timestamp_ms: fetch_end,
            stream_id: stream_id_val,
            response_bytes: response_bytes.as_ref().map_or(0, |b| b.len()),
            latency_ms: fetch_end - fetch_start,
        });

        match response_bytes {
            Some(bytes) => {
                let batch_response: BatchCommandResponse = match borsh::from_slice(&bytes) {
                    Ok(r) => r,
                    Err(e) => {
                        let err = JsValue::from_str(&format!("deserialize batch response: {e}"));
                        for item in &items {
                            let _ = item.reject.call1(&JsValue::NULL, &err);
                        }
                        finish_flush(stream_id_val);
                        return;
                    }
                };

                // Process responses and check for rejections
                let mut any_rejected = false;
                let mut reject_reason = String::new();

                for response in batch_response.responses {
                    if let Verdict::Rejected { ref reason } = response.verdict {
                        if !any_rejected {
                            any_rejected = true;
                            reject_reason = reason.clone();
                        }
                    }
                    let _ = CLIENT.with(|c| {
                        let mut borrow = c.borrow_mut();
                        if let Some(client) = borrow.as_mut() {
                            let _ = client.receive_response(response);
                        }
                    });
                }

                // Resolve/reject all item promises
                if any_rejected {
                    log_event(DebugEvent::Rejected {
                        timestamp_ms: now_ms(),
                        stream_id: stream_id_val,
                        reason: reject_reason.clone(),
                    });
                    let result = js_sys::Object::new();
                    let _ = js_sys::Reflect::set(&result, &"status".into(), &"rejected".into());
                    let _ = js_sys::Reflect::set(&result, &"reason".into(), &reject_reason.into());
                    let result_val: JsValue = result.into();
                    for item in &items {
                        let _ = item.resolve.call1(&JsValue::NULL, &result_val);
                    }
                } else {
                    log_event(DebugEvent::Confirmed {
                        timestamp_ms: now_ms(),
                        stream_id: stream_id_val,
                    });
                    let result = js_sys::Object::new();
                    let _ = js_sys::Reflect::set(&result, &"status".into(), &"confirmed".into());
                    let result_val: JsValue = result.into();
                    for item in &items {
                        let _ = item.resolve.call1(&JsValue::NULL, &result_val);
                    }
                }
                notify_all();
            }
            None => {
                // All retries exhausted
                let err = last_err.unwrap_or_else(|| JsValue::from_str("fetch failed"));
                for item in &items {
                    let _ = item.reject.call1(&JsValue::NULL, &err);
                }
            }
        }

        finish_flush(stream_id_val);
    });
}

fn finish_flush(stream_id_val: u64) {
    STREAM_HANDLES.with(|sh| {
        let mut sh = sh.borrow_mut();
        if let Some(handle) = sh.get_mut(&stream_id_val) {
            handle.in_flight = false;

            // Resolve flush waiters if queue is empty and not in-flight
            if handle.queue.is_empty() {
                let waiters: Vec<js_sys::Function> = handle.flush_waiters.drain(..).collect();
                for waiter in &waiters {
                    let _ = waiter.call0(&JsValue::NULL);
                }
            }
        }
    });
    try_drain_queue(stream_id_val);
}

// ── Exported API ─────────────────────────────────────────────────

#[wasm_bindgen]
pub fn init() {
    CLIENT.with(|c| {
        let mut client = SyncClient::new(make_db());
        let stream_id = client.create_stream();
        let stream_id_val = stream_id.0;
        *c.borrow_mut() = Some(client);

        STREAM_HANDLES.with(|sh| {
            sh.borrow_mut().insert(stream_id_val, StreamHandle {
                stream_id,
                batch_count: 1,
                batch_wait_ms: 0,
                retry_count: 0,
                queue: Vec::new(),
                in_flight: false,
                flush_waiters: Vec::new(),
                microtask_scheduled: false,
            });
        });
        DEFAULT_STREAM_ID.with(|d| *d.borrow_mut() = Some(stream_id_val));
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

/// Create a new stream with batching configuration.
/// batch_count=1 means sequential (one request at a time).
#[wasm_bindgen]
pub fn create_stream(batch_count: u32, batch_wait_ms: u32, retry_count: u32) -> f64 {
    with_client(|client| {
        let stream_id = client.create_stream();
        let stream_id_val = stream_id.0;
        STREAM_HANDLES.with(|sh| {
            sh.borrow_mut().insert(stream_id_val, StreamHandle {
                stream_id,
                batch_count: (batch_count as usize).max(1),
                batch_wait_ms,
                retry_count,
                queue: Vec::new(),
                in_flight: false,
                flush_waiters: Vec::new(),
                microtask_scheduled: false,
            });
        });
        stream_id_val as f64
    })
}

/// Register a reactive query subscription. Parses the SQL, builds an execution plan
/// with reactive metadata, and registers with the subscription registry.
#[wasm_bindgen]
pub fn subscribe(sql: &str, callback: js_sys::Function) -> f64 {
    let stmt = sql_parser::parser::parse_statement(sql)
        .unwrap_or_else(|e| panic!("subscribe: parse error: {}", e));
    let select = match stmt {
        sql_parser::ast::Statement::Select(s) => s,
        _ => panic!("subscribe only supports SELECT statements"),
    };

    let (sub_id, tables) = with_client(|client| {
        let table_schemas = client.db().table_schemas();
        let conditions = sql_engine::reactive::plan_reactive(&select, &table_schemas)
            .unwrap_or_else(|e| panic!("subscribe: plan_reactive error: {e:?}"));

        let tables: Vec<String> = select.sources.iter().map(|s| s.table.clone()).collect();

        let sub_id = REGISTRY.with(|r| r.borrow_mut().subscribe(&conditions));
        (sub_id, tables)
    });

    CALLBACKS.with(|cbs| cbs.borrow_mut().insert(sub_id.0, callback));
    SUB_SQL.with(|s| s.borrow_mut().insert(sub_id.0, sql.to_string()));
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
    REGISTRY.with(|r| r.borrow_mut().unsubscribe(id));
    CALLBACKS.with(|cbs| cbs.borrow_mut().remove(&(sub_id as u64)));
    SUB_SQL.with(|s| s.borrow_mut().remove(&(sub_id as u64)));
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

    // Optimistically execute (synchronous)
    let request = with_client(|client| {
        let sid = sync::protocol::StreamId(stream_id_val);
        client
            .execute(sid, cmd)
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

    // Notify affected subscriptions (optimistic state changed)
    notify_affected(&request.client_zset);

    // Create Promise with manual resolve/reject control
    let resolve_slot: Rc<RefCell<Option<js_sys::Function>>> = Rc::new(RefCell::new(None));
    let reject_slot: Rc<RefCell<Option<js_sys::Function>>> = Rc::new(RefCell::new(None));
    let rs = resolve_slot.clone();
    let rj = reject_slot.clone();
    let confirmed = js_sys::Promise::new(&mut move |resolve, reject| {
        *rs.borrow_mut() = Some(resolve);
        *rj.borrow_mut() = Some(reject);
    });
    let resolve = resolve_slot.borrow_mut().take().unwrap();
    let reject = reject_slot.borrow_mut().take().unwrap();

    // Queue for batching
    STREAM_HANDLES.with(|sh| {
        let mut sh = sh.borrow_mut();
        let handle = sh.get_mut(&stream_id_val)
            .expect("unknown stream — call create_stream() first");
        handle.queue.push(PendingFetch { request, resolve, reject });
    });

    try_drain_queue(stream_id_val);

    // Return { zset, confirmed }
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

/// Flush all queued commands on a stream. Returns a Promise that resolves
/// when all commands are confirmed/rejected.
#[wasm_bindgen]
pub fn flush_stream(stream_id: f64) -> js_sys::Promise {
    let stream_id_val = stream_id as u64;

    let is_done = STREAM_HANDLES.with(|sh| {
        let sh = sh.borrow();
        let handle = match sh.get(&stream_id_val) {
            Some(h) => h,
            None => return true,
        };
        handle.queue.is_empty() && !handle.in_flight
    });

    if is_done {
        return js_sys::Promise::resolve(&JsValue::UNDEFINED);
    }

    // Create Promise, store resolver as flush waiter
    let resolve_slot: Rc<RefCell<Option<js_sys::Function>>> = Rc::new(RefCell::new(None));
    let rs = resolve_slot.clone();
    let promise = js_sys::Promise::new(&mut move |resolve, _reject| {
        *rs.borrow_mut() = Some(resolve);
    });
    let resolve = resolve_slot.borrow_mut().take().unwrap();

    STREAM_HANDLES.with(|sh| {
        let mut sh = sh.borrow_mut();
        if let Some(handle) = sh.get_mut(&stream_id_val) {
            handle.flush_waiters.push(resolve);
        }
    });

    // If not in-flight, flush all items now
    let not_in_flight = STREAM_HANDLES.with(|sh| {
        sh.borrow().get(&stream_id_val).map_or(true, |h| !h.in_flight)
    });
    if not_in_flight {
        do_flush_stream(stream_id_val, true);
    }

    promise
}

/// Execute a SQL query against the optimistic (local) database.
/// Returns a row-major array of arrays, e.g. `[[1, "Alice", 30], [2, "Bob", 25]]`.
#[wasm_bindgen]
pub fn query(sql: &str) -> Result<JsValue, JsError> {
    // Look up triggered conditions for this SQL's subscription.
    let triggered = find_triggered_for_sql(sql);

    with_client(|client| {
        let (columns, spans) = client
            .db_mut()
            .execute_traced_with_triggered(sql, triggered)
            .map_err(|e| JsError::new(&e.to_string()))?;
        let rows = columns_to_rows(columns);
        let row_count = rows.len();
        let duration_us = spans.iter().map(|s| s.duration.as_micros() as u64).sum::<u64>();
        let is_slow = duration_us > SLOW_QUERY_THRESHOLD_US;

        QUERY_LOG.with(|ql| {
            ql.borrow_mut().push(QueryTrace {
                timestamp_ms: now_ms(),
                sql: sql.to_string(),
                duration_us,
                row_count,
                source: "optimistic".into(),
                spans,
                is_slow,
            });
        });

        log_event(DebugEvent::QueryExecuted {
            timestamp_ms: now_ms(),
            sql: sql.to_string(),
            duration_us,
            row_count,
            source: "optimistic".into(),
        });

        if is_slow {
            log_event(DebugEvent::SlowQuery {
                timestamp_ms: now_ms(),
                sql: sql.to_string(),
                duration_us,
            });
        }

        serde_wasm_bindgen::to_value(&rows).map_err(|e| JsError::new(&e.to_string()))
    })
}

/// Execute a SQL query against the confirmed (server-acknowledged) database.
#[wasm_bindgen]
pub fn query_confirmed(sql: &str) -> Result<JsValue, JsError> {
    let triggered = find_triggered_for_sql(sql);

    with_client(|client| {
        let (columns, spans) = client
            .confirmed_db_mut()
            .execute_traced_with_triggered(sql, triggered)
            .map_err(|e| JsError::new(&e.to_string()))?;
        let rows = columns_to_rows(columns);
        let row_count = rows.len();
        let duration_us = spans.iter().map(|s| s.duration.as_micros() as u64).sum::<u64>();
        let is_slow = duration_us > SLOW_QUERY_THRESHOLD_US;

        QUERY_LOG.with(|ql| {
            ql.borrow_mut().push(QueryTrace {
                timestamp_ms: now_ms(),
                sql: sql.to_string(),
                duration_us,
                row_count,
                source: "confirmed".into(),
                spans,
                is_slow,
            });
        });

        log_event(DebugEvent::QueryExecuted {
            timestamp_ms: now_ms(),
            sql: sql.to_string(),
            duration_us,
            row_count,
            source: "confirmed".into(),
        });

        if is_slow {
            log_event(DebugEvent::SlowQuery {
                timestamp_ms: now_ms(),
                sql: sql.to_string(),
                duration_us,
            });
        }

        serde_wasm_bindgen::to_value(&rows).map_err(|e| JsError::new(&e.to_string()))
    })
}

// ── Debug exports ──────────────────────────────────────────────────

#[wasm_bindgen]
pub fn debug_event_log() -> Result<JsValue, JsError> {
    DEBUG_LOG.with(|log| {
        let log = log.borrow();
        let events: Vec<&DebugEvent> = log.drain_ordered();
        serde_wasm_bindgen::to_value(&events)
            .map_err(|e| JsError::new(&e.to_string()))
    })
}

#[wasm_bindgen]
pub fn debug_sync_status() -> Result<JsValue, JsError> {
    with_client(|client| {
        #[derive(Serialize)]
        struct PendingDetail { seq_no: u64, zset_entries: usize }

        #[derive(Serialize)]
        struct StreamInfo {
            id: u64,
            pending_count: usize,
            is_idle: bool,
            pending: Vec<PendingDetail>,
        }

        #[derive(Serialize)]
        struct SyncStatus {
            stream_count: usize,
            total_pending: usize,
            streams: Vec<StreamInfo>,
        }

        let detail = client.stream_pending_detail();
        let status = SyncStatus {
            stream_count: client.stream_count(),
            total_pending: client.total_pending(),
            streams: detail.iter().map(|(id, entries)| StreamInfo {
                id: id.0,
                pending_count: entries.len(),
                is_idle: entries.is_empty(),
                pending: entries.iter().map(|(seq, zset_len)| PendingDetail {
                    seq_no: *seq,
                    zset_entries: *zset_len,
                }).collect(),
            }).collect(),
        };
        serde_wasm_bindgen::to_value(&status)
            .map_err(|e| JsError::new(&e.to_string()))
    })
}

#[wasm_bindgen]
pub fn debug_subscriptions() -> Result<JsValue, JsError> {
    #[derive(Serialize)]
    struct SubInfo { id: u64, sql: String, tables: Vec<String>, notification_count: u64 }

    #[derive(Serialize)]
    struct SubscriptionDebug {
        count: usize,
        subscriptions: Vec<SubInfo>,
        reverse_index_size: usize,
    }

    let (count, table_subs, reverse_index_size) = REGISTRY.with(|r| {
        let reg = r.borrow();
        (reg.subscription_count(), reg.table_subscriptions().clone(), reg.reverse_index_size())
    });

    let notification_counts = NOTIFICATION_COUNTS.with(|nc| nc.borrow().clone());
    let sub_sqls = SUB_SQL.with(|s| s.borrow().clone());

    // Invert table_subs: table->[SubId] → SubId->[tables]
    let mut sub_tables: HashMap<u64, Vec<String>> = HashMap::new();
    for (table, subs) in &table_subs {
        for sub_id in subs {
            sub_tables.entry(sub_id.0).or_default().push(table.clone());
        }
    }
    // Include SubIds from CALLBACKS that might not be in table_subs
    CALLBACKS.with(|cbs| {
        for id in cbs.borrow().keys() {
            sub_tables.entry(*id).or_default();
        }
    });

    let debug = SubscriptionDebug {
        count,
        subscriptions: sub_tables.iter().map(|(id, tables)| SubInfo {
            id: *id,
            sql: sub_sqls.get(id).cloned().unwrap_or_default(),
            tables: tables.clone(),
            notification_count: notification_counts.get(id).copied().unwrap_or(0),
        }).collect(),
        reverse_index_size,
    };

    serde_wasm_bindgen::to_value(&debug)
        .map_err(|e| JsError::new(&e.to_string()))
}

#[wasm_bindgen]
pub fn debug_database() -> Result<JsValue, JsError> {
    with_client(|client| {
        #[derive(Serialize)]
        struct ColumnInfo { name: String, data_type: String, nullable: bool }

        #[derive(Serialize)]
        struct IndexInfo { columns: Vec<String>, index_type: String, key_count: usize }

        #[derive(Serialize)]
        struct TableInfo {
            name: String,
            row_count: usize,
            physical_len: usize,
            deleted_count: usize,
            fragmentation_ratio: f64,
            columns: Vec<ColumnInfo>,
            index_count: usize,
            indexes: Vec<IndexInfo>,
            estimated_memory_bytes: usize,
        }

        #[derive(Serialize)]
        struct DbInfo { tables: Vec<TableInfo> }

        #[derive(Serialize)]
        struct DatabaseDebug { optimistic: DbInfo, confirmed: DbInfo }

        fn estimate_table_memory(t: &sql_engine::storage::Table) -> usize {
            let mut bytes = 0usize;
            for col in &t.columns {
                bytes += match col {
                    TypedColumn::I64(v) => v.len() * 8,
                    TypedColumn::Str(v) => v.iter().map(|s| s.len() + 24).sum::<usize>(),
                    TypedColumn::NullableI64 { values, .. } => values.len() * 8 + values.len() / 8,
                    TypedColumn::NullableStr { values, .. } => values.iter().map(|s| s.len() + 24).sum::<usize>() + values.len() / 8,
                };
            }
            for idx in t.indexes() {
                bytes += idx.key_count() * idx.columns().len() * 16;
            }
            bytes
        }

        fn db_info(db: &Database) -> DbInfo {
            let names = db.table_names();
            let tables = names.iter().map(|name| {
                let t = db.table(name).unwrap();
                let physical = t.physical_len();
                let deleted = t.deleted_count();
                TableInfo {
                    name: name.clone(),
                    row_count: t.len(),
                    physical_len: physical,
                    deleted_count: deleted,
                    fragmentation_ratio: if physical > 0 { deleted as f64 / physical as f64 } else { 0.0 },
                    columns: t.schema.columns.iter().map(|c| ColumnInfo {
                        name: c.name.clone(),
                        data_type: format!("{:?}", c.data_type),
                        nullable: c.nullable,
                    }).collect(),
                    index_count: t.indexes().len(),
                    indexes: t.indexes().iter().map(|idx| {
                        let col_names: Vec<String> = idx.columns().iter().map(|&ci| {
                            t.schema.columns.get(ci).map(|c| c.name.clone()).unwrap_or_else(|| format!("col{ci}"))
                        }).collect();
                        IndexInfo {
                            columns: col_names,
                            index_type: if idx.is_hash() { "Hash".into() } else { "BTree".into() },
                            key_count: idx.key_count(),
                        }
                    }).collect(),
                    estimated_memory_bytes: estimate_table_memory(t),
                }
            }).collect();
            DbInfo { tables }
        }

        let debug = DatabaseDebug {
            optimistic: db_info(client.db()),
            confirmed: db_info(client.confirmed_db()),
        };
        serde_wasm_bindgen::to_value(&debug)
            .map_err(|e| JsError::new(&e.to_string()))
    })
}

#[wasm_bindgen]
pub fn debug_table_rows(table_name: &str, db_kind: &str, limit: usize) -> Result<JsValue, JsError> {
    with_client(|client| {
        let db = match db_kind {
            "confirmed" => client.confirmed_db(),
            _ => client.db(),
        };
        let table = db.table(table_name)
            .ok_or_else(|| JsError::new(&format!("table not found: {table_name}")))?;

        let col_count = table.schema.columns.len();
        let rows: Vec<Vec<CellValue>> = table.row_ids()
            .take(limit)
            .map(|row_id| {
                (0..col_count).map(|col| table.get(row_id, col)).collect()
            })
            .collect();

        serde_wasm_bindgen::to_value(&rows)
            .map_err(|e| JsError::new(&e.to_string()))
    })
}

#[wasm_bindgen]
pub fn debug_wasm_memory() -> usize {
    let mem = wasm_bindgen::memory();
    let buf = js_sys::Reflect::get(&mem, &"buffer".into()).unwrap_or(JsValue::NULL);
    let len = js_sys::Reflect::get(&buf, &"byteLength".into()).unwrap_or(JsValue::from(0));
    len.as_f64().unwrap_or(0.0) as usize
}

#[wasm_bindgen]
pub fn debug_event_count() -> u64 {
    DEBUG_LOG.with(|log| log.borrow().total_count)
}

#[wasm_bindgen]
pub fn debug_clear_log() {
    DEBUG_LOG.with(|log| {
        let mut log = log.borrow_mut();
        log.events.clear();
        log.write_pos = 0;
    });
    QUERY_LOG.with(|ql| {
        let mut ql = ql.borrow_mut();
        ql.queries.clear();
        ql.write_pos = 0;
        ql.total_count = 0;
        ql.slow_count = 0;
    });
    TABLE_INVALIDATION_COUNTS.with(|tc| tc.borrow_mut().clear());
    NOTIFICATION_COUNTS.with(|nc| nc.borrow_mut().clear());
}

#[wasm_bindgen]
pub fn debug_query_log() -> Result<JsValue, JsError> {
    QUERY_LOG.with(|ql| {
        let ql = ql.borrow();
        let queries: Vec<&QueryTrace> = ql.drain_ordered();
        serde_wasm_bindgen::to_value(&queries)
            .map_err(|e| JsError::new(&e.to_string()))
    })
}

#[wasm_bindgen]
pub fn debug_query_stats() -> Result<JsValue, JsError> {
    #[derive(Serialize)]
    struct QueryStats {
        total_queries: u64,
        slow_queries: u64,
        table_invalidation_counts: HashMap<String, u64>,
    }

    let (total, slow) = QUERY_LOG.with(|ql| {
        let ql = ql.borrow();
        (ql.total_count, ql.slow_count)
    });
    let table_counts = TABLE_INVALIDATION_COUNTS.with(|tc| tc.borrow().clone());

    let stats = QueryStats {
        total_queries: total,
        slow_queries: slow,
        table_invalidation_counts: table_counts,
    };
    serde_wasm_bindgen::to_value(&stats)
        .map_err(|e| JsError::new(&e.to_string()))
}
