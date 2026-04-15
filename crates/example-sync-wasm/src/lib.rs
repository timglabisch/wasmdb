use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use js_sys::Uint8Array;
use database::Database;
use serde::Serialize;
use sql_engine::schema::{ColumnSchema, DataType, TableSchema};
use sql_engine::storage::CellValue;
use sql_engine::reactive::{SubscriptionRegistry, SubId};
use sync::protocol::CommandResponse;
use sync::zset::ZSet;
use sync_client::client::SyncClient;
use sync_client::stream::StreamAction;
use example_sync_commands::UserCommand;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

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

// ── Thread locals ──────────────────────────────────────────────────

thread_local! {
    static CLIENT: RefCell<Option<SyncClient<UserCommand>>> = RefCell::new(None);
    static REGISTRY: RefCell<SubscriptionRegistry> = RefCell::new(SubscriptionRegistry::new());
    static CALLBACKS: RefCell<HashMap<u64, js_sys::Function>> = RefCell::new(HashMap::new());
    static ID_COUNTER: RefCell<i64> = RefCell::new(0);
    static DEBUG_LOG: RefCell<EventLog> = RefCell::new(EventLog::new());
    static NOTIFICATION_COUNTS: RefCell<HashMap<u64, u64>> = RefCell::new(HashMap::new());
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

// ── Reactive notification ───────────────────────────────────────

/// Determine affected subscriptions from a ZSet and invoke their callbacks.
fn notify_affected(zset: &ZSet) {
    let affected = REGISTRY.with(|r| {
        let reg = r.borrow();
        let mut affected = HashSet::new();
        for entry in &zset.entries {
            if entry.weight > 0 {
                affected.extend(reg.on_insert(&entry.table, &entry.row));
            } else {
                affected.extend(reg.on_delete(&entry.table, &entry.row));
            }
        }
        affected
    });

    if !affected.is_empty() {
        let sub_ids: Vec<u64> = affected.iter().map(|s| s.0).collect();
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
    }

    CALLBACKS.with(|cbs| {
        let cbs = cbs.borrow();
        for sub_id in &affected {
            if let Some(f) = cbs.get(&sub_id.0) {
                let _ = f.call0(&JsValue::NULL);
            }
        }
    });
}

/// Notify all subscribers (used after server response / rollback).
fn notify_all() {
    CALLBACKS.with(|cbs| {
        for f in cbs.borrow().values() {
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

fn columns_to_rows(columns: Vec<Vec<CellValue>>) -> Vec<Vec<CellValue>> {
    if columns.is_empty() || columns[0].is_empty() {
        return vec![];
    }
    let num_rows = columns[0].len();
    (0..num_rows)
        .map(|i| columns.iter().map(|col| col[i].clone()).collect())
        .collect()
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

/// Extract table names from a SQL SELECT statement.
fn extract_tables(sql: &str) -> Vec<String> {
    let stmt = match sql_parser::parser::parse_statement(sql) {
        Ok(s) => s,
        Err(_) => return vec![],
    };
    match stmt {
        sql_parser::ast::Statement::Select(select) => {
            select.sources.iter().map(|s| s.table.clone()).collect()
        }
        _ => vec![],
    }
}

// ── Exported API ─────────────────────────────────────────────────

#[wasm_bindgen]
pub fn init() {
    CLIENT.with(|c| {
        *c.borrow_mut() = Some(SyncClient::new(make_db()));
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

/// Register a reactive query subscription. Parses the SQL to determine
/// which tables to watch, stores the callback, and returns a subscription ID.
#[wasm_bindgen]
pub fn subscribe(sql: &str, callback: js_sys::Function) -> f64 {
    let tables = extract_tables(sql);
    let sub_id = REGISTRY.with(|r| r.borrow_mut().subscribe_tables(&tables));
    CALLBACKS.with(|cbs| cbs.borrow_mut().insert(sub_id.0, callback));
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
    log_event(DebugEvent::SubscriptionRemoved {
        timestamp_ms: now_ms(),
        sub_id: sub_id as u64,
    });
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

    let stream_id_val = request.stream_id.0;
    log_event(DebugEvent::Execute {
        timestamp_ms: now_ms(),
        stream_id: stream_id_val,
        command_json: cmd_json.to_string(),
        zset_entry_count: request.client_zset.len(),
    });

    // ZSet as JsValue via serde-wasm-bindgen
    let zset_js = serde_wasm_bindgen::to_value(&request.client_zset)
        .map_err(|e| JsError::new(&e.to_string()))?;

    // Borsh bytes for the server
    let request_bytes = borsh::to_vec(&request).map_err(|e| JsError::new(&e.to_string()))?;

    // Notify affected subscriptions (optimistic state changed)
    notify_affected(&request.client_zset);

    // Build Promise for server confirmation
    let request_bytes_len = request_bytes.len();
    let confirmed =
        wasm_bindgen_futures::future_to_promise(async move {
            log_event(DebugEvent::FetchStart {
                timestamp_ms: now_ms(),
                stream_id: stream_id_val,
                request_bytes: request_bytes_len,
            });

            let fetch_start = now_ms();
            let response_bytes = do_fetch(&request_bytes).await?;
            let fetch_end = now_ms();

            log_event(DebugEvent::FetchEnd {
                timestamp_ms: fetch_end,
                stream_id: stream_id_val,
                response_bytes: response_bytes.len(),
                latency_ms: fetch_end - fetch_start,
            });

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

            match &action {
                StreamAction::Rejected { reason, .. } => {
                    log_event(DebugEvent::Rejected {
                        timestamp_ms: now_ms(),
                        stream_id: stream_id_val,
                        reason: reason.clone(),
                    });
                    notify_all();
                }
                _ => {
                    log_event(DebugEvent::Confirmed {
                        timestamp_ms: now_ms(),
                        stream_id: stream_id_val,
                    });
                    notify_all();
                }
            }

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

/// Execute a SQL query against the optimistic (local) database.
/// Returns a row-major array of arrays, e.g. `[[1, "Alice", 30], [2, "Bob", 25]]`.
#[wasm_bindgen]
pub fn query(sql: &str) -> Result<JsValue, JsError> {
    with_client(|client| {
        let columns = client
            .db_mut()
            .execute(sql)
            .map_err(|e| JsError::new(&e.to_string()))?;
        let rows = columns_to_rows(columns);
        serde_wasm_bindgen::to_value(&rows).map_err(|e| JsError::new(&e.to_string()))
    })
}

/// Execute a SQL query against the confirmed (server-acknowledged) database.
#[wasm_bindgen]
pub fn query_confirmed(sql: &str) -> Result<JsValue, JsError> {
    with_client(|client| {
        let columns = client
            .confirmed_db_mut()
            .execute(sql)
            .map_err(|e| JsError::new(&e.to_string()))?;
        let rows = columns_to_rows(columns);
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
    struct SubInfo { id: u64, tables: Vec<String> }

    #[derive(Serialize)]
    struct SubscriptionDebug {
        count: usize,
        subscriptions: Vec<SubInfo>,
        notification_counts: HashMap<u64, u64>,
        reverse_index_size: usize,
    }

    let (count, table_subs, reverse_index_size) = REGISTRY.with(|r| {
        let reg = r.borrow();
        (reg.subscription_count(), reg.table_subscriptions().clone(), reg.reverse_index_size())
    });

    let notification_counts = NOTIFICATION_COUNTS.with(|nc| nc.borrow().clone());

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
            tables: tables.clone(),
        }).collect(),
        notification_counts,
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
        struct TableInfo {
            name: String,
            row_count: usize,
            physical_len: usize,
            columns: Vec<ColumnInfo>,
            index_count: usize,
        }

        #[derive(Serialize)]
        struct DbInfo { tables: Vec<TableInfo> }

        #[derive(Serialize)]
        struct DatabaseDebug { optimistic: DbInfo, confirmed: DbInfo }

        fn db_info(db: &Database) -> DbInfo {
            let names = db.table_names();
            let tables = names.iter().map(|name| {
                let t = db.table(name).unwrap();
                TableInfo {
                    name: name.clone(),
                    row_count: t.len(),
                    physical_len: t.physical_len(),
                    columns: t.schema.columns.iter().map(|c| ColumnInfo {
                        name: c.name.clone(),
                        data_type: format!("{:?}", c.data_type),
                        nullable: c.nullable,
                    }).collect(),
                    index_count: t.indexes().len(),
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
}
