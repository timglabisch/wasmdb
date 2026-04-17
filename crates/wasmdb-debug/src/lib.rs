//! Generic debug-event + query-trace collection for wasmdb-based apps.
//!
//! This crate holds the pure-Rust instrumentation layer that the example
//! (and any future wasmdb app) feeds into. It has no wasm-bindgen or
//! SyncClient coupling — callers push events in, and read snapshots out.

use std::cell::RefCell;
use std::collections::HashMap;

use serde::Serialize;
use sql_engine::execute::Span;
use sync::zset::ZSet;

// ── Event log ───────────────────────────────────────────────────────

#[derive(Clone, Serialize)]
#[serde(tag = "kind")]
pub enum DebugEvent {
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
pub struct QueryTrace {
    pub timestamp_ms: f64,
    pub sql: String,
    pub duration_us: u64,
    pub row_count: usize,
    pub source: String,
    pub spans: Vec<Span>,
    pub is_slow: bool,
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
    static QUERY_LOG_TL: RefCell<QueryLog> = RefCell::new(QueryLog::new());
    static NOTIFICATION_COUNTS: RefCell<HashMap<u64, u64>> = RefCell::new(HashMap::new());
    static TABLE_INVALIDATION_COUNTS: RefCell<HashMap<String, u64>> = RefCell::new(HashMap::new());
}

// ── Write API ───────────────────────────────────────────────────────

pub fn log_event(event: DebugEvent) {
    DEBUG_LOG.with(|log| log.borrow_mut().push(event));
}

pub fn bump_notification_count(sub_id: u64) {
    NOTIFICATION_COUNTS.with(|nc| {
        *nc.borrow_mut().entry(sub_id).or_insert(0) += 1;
    });
}

pub fn track_table_invalidations(zset: &ZSet) {
    TABLE_INVALIDATION_COUNTS.with(|tc| {
        let mut tc = tc.borrow_mut();
        for entry in &zset.entries {
            *tc.entry(entry.table.clone()).or_insert(0) += 1;
        }
    });
}

/// Record a completed query in both the query log and the event stream.
/// Emits an additional `SlowQuery` event when duration exceeds the threshold.
pub fn record_query(timestamp_ms: f64, sql: &str, source: &str, spans: Vec<Span>, row_count: usize) {
    let duration_us = spans.iter().map(|s| s.duration.as_micros() as u64).sum::<u64>();
    let is_slow = duration_us > SLOW_QUERY_THRESHOLD_US;

    QUERY_LOG_TL.with(|ql| {
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

pub fn clear() {
    DEBUG_LOG.with(|log| {
        let mut log = log.borrow_mut();
        log.events.clear();
        log.write_pos = 0;
    });
    QUERY_LOG_TL.with(|ql| {
        let mut ql = ql.borrow_mut();
        ql.queries.clear();
        ql.write_pos = 0;
        ql.total_count = 0;
        ql.slow_count = 0;
    });
    TABLE_INVALIDATION_COUNTS.with(|tc| tc.borrow_mut().clear());
    NOTIFICATION_COUNTS.with(|nc| nc.borrow_mut().clear());
}

// ── Read API ────────────────────────────────────────────────────────

pub fn snapshot_events() -> Vec<DebugEvent> {
    DEBUG_LOG.with(|log| log.borrow().drain_ordered().into_iter().cloned().collect())
}

pub fn event_count() -> u64 {
    DEBUG_LOG.with(|log| log.borrow().total_count)
}

pub fn snapshot_queries() -> Vec<QueryTrace> {
    QUERY_LOG_TL.with(|ql| ql.borrow().drain_ordered().into_iter().cloned().collect())
}

pub fn query_totals() -> (u64, u64) {
    QUERY_LOG_TL.with(|ql| {
        let ql = ql.borrow();
        (ql.total_count, ql.slow_count)
    })
}

pub fn snapshot_notification_counts() -> HashMap<u64, u64> {
    NOTIFICATION_COUNTS.with(|nc| nc.borrow().clone())
}

pub fn snapshot_table_invalidations() -> HashMap<String, u64> {
    TABLE_INVALIDATION_COUNTS.with(|tc| tc.borrow().clone())
}
