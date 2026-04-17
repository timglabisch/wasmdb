use std::cell::RefCell;
use std::collections::HashMap;

use database::Database;
use database_reactive::SubId;
use serde::Serialize;
use sql_engine::execute::Span;
use sql_engine::storage::{CellValue, TypedColumn};
use sync::zset::ZSet;
use wasm_bindgen::prelude::*;

use crate::state::with_client;

// ── Event log ───────────────────────────────────────────────────────

#[derive(Clone, Serialize)]
#[serde(tag = "kind")]
pub(crate) enum DebugEvent {
    Execute { timestamp_ms: f64, stream_id: u64, command_json: String, zset_entry_count: usize },
    FetchStart { timestamp_ms: f64, stream_id: u64, request_bytes: usize },
    FetchEnd { timestamp_ms: f64, stream_id: u64, response_bytes: usize, latency_ms: f64 },
    Confirmed { timestamp_ms: f64, stream_id: u64 },
    Rejected { timestamp_ms: f64, stream_id: u64, reason: String },
    Notification { timestamp_ms: f64, sub_id: u64, triggered_count: usize },
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

// ── Query trace log ─────────────────────────────────────────────────

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
const SLOW_QUERY_THRESHOLD_US: u64 = 10_000;

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

// ── Thread locals ───────────────────────────────────────────────────

thread_local! {
    static DEBUG_LOG: RefCell<EventLog> = RefCell::new(EventLog::new());
    static QUERY_LOG: RefCell<QueryLog> = RefCell::new(QueryLog::new());
    static NOTIFICATION_COUNTS: RefCell<HashMap<u64, u64>> = RefCell::new(HashMap::new());
    static TABLE_INVALIDATION_COUNTS: RefCell<HashMap<String, u64>> = RefCell::new(HashMap::new());
}

// ── Crate-private helpers ───────────────────────────────────────────

pub(crate) fn now_ms() -> f64 {
    js_sys::Date::now()
}

pub(crate) fn log_event(event: DebugEvent) {
    DEBUG_LOG.with(|log| log.borrow_mut().push(event));
}

pub(crate) fn bump_notification_count(sub_id: u64) {
    NOTIFICATION_COUNTS.with(|nc| {
        *nc.borrow_mut().entry(sub_id).or_insert(0) += 1;
    });
}

pub(crate) fn track_table_invalidations(zset: &ZSet) {
    TABLE_INVALIDATION_COUNTS.with(|tc| {
        let mut tc = tc.borrow_mut();
        for entry in &zset.entries {
            *tc.entry(entry.table.clone()).or_insert(0) += 1;
        }
    });
}

/// Record a completed query in both the query log and the event stream.
/// Emits an additional `SlowQuery` event when duration exceeds the threshold.
pub(crate) fn record_query(sql: &str, source: &str, spans: Vec<Span>, row_count: usize) {
    let duration_us = spans.iter().map(|s| s.duration.as_micros() as u64).sum::<u64>();
    let is_slow = duration_us > SLOW_QUERY_THRESHOLD_US;
    let timestamp_ms = now_ms();

    QUERY_LOG.with(|ql| {
        ql.borrow_mut().push(QueryTrace {
            timestamp_ms,
            sql: sql.to_string(),
            duration_us,
            row_count,
            source: source.to_string(),
            spans,
            is_slow,
        });
    });

    log_event(DebugEvent::QueryExecuted {
        timestamp_ms,
        sql: sql.to_string(),
        duration_us,
        row_count,
        source: source.to_string(),
    });

    if is_slow {
        log_event(DebugEvent::SlowQuery {
            timestamp_ms,
            sql: sql.to_string(),
            duration_us,
        });
    }
}

// ── WASM exports ────────────────────────────────────────────────────

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

    with_client(|client| {
        let rdb = client.db();
        let reg = rdb.registry();
        let table_subs = reg.table_subscriptions().clone();
        let count = rdb.subscription_count();
        let reverse_index_size = reg.reverse_index_size();

        let notification_counts = NOTIFICATION_COUNTS.with(|nc| nc.borrow().clone());

        let mut sub_tables: HashMap<u64, Vec<String>> = HashMap::new();
        for (table, subs) in &table_subs {
            for sub_id in subs {
                sub_tables.entry(sub_id.0).or_default().push(table.clone());
            }
        }
        for id in rdb.subscription_ids() {
            sub_tables.entry(id.0).or_default();
        }

        let debug = SubscriptionDebug {
            count,
            subscriptions: sub_tables.iter().map(|(id, tables)| SubInfo {
                id: *id,
                sql: rdb.subscription_sql(SubId(*id)).unwrap_or("").to_string(),
                tables: tables.clone(),
                notification_count: notification_counts.get(id).copied().unwrap_or(0),
            }).collect(),
            reverse_index_size,
        };

        serde_wasm_bindgen::to_value(&debug)
            .map_err(|e| JsError::new(&e.to_string()))
    })
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
            optimistic: db_info(client.db().db()),
            confirmed: db_info(client.confirmed_db()),
        };
        serde_wasm_bindgen::to_value(&debug)
            .map_err(|e| JsError::new(&e.to_string()))
    })
}

#[wasm_bindgen]
pub fn debug_table_rows(table_name: &str, db_kind: &str, limit: usize) -> Result<JsValue, JsError> {
    with_client(|client| {
        let db: &Database = match db_kind {
            "confirmed" => client.confirmed_db(),
            _ => client.db().db(),
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
