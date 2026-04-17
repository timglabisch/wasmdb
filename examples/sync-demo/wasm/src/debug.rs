//! Wasm-bindgen bridge for the debug toolbar.
//!
//! Pure-Rust instrumentation (event log, query trace log, counters) lives in
//! the `wasmdb-debug` crate. This module only wires the wasm-bindgen exports
//! and the few readers that need access to the `SyncClient<UserCommand>`.

use std::collections::HashMap;

use database::Database;
use database_reactive::SubId;
use serde::Serialize;
use sql_engine::execute::Span;
use sql_engine::storage::{CellValue, TypedColumn};
use sync::zset::ZSet;
use wasm_bindgen::prelude::*;

pub(crate) use wasmdb_debug::DebugEvent;

use crate::state::with_client;

// ── Crate-private write helpers (used by api.rs, stream.rs, reactive.rs) ──

pub(crate) fn now_ms() -> f64 {
    js_sys::Date::now()
}

pub(crate) fn log_event(event: DebugEvent) {
    wasmdb_debug::log_event(event);
}

pub(crate) fn bump_notification_count(sub_id: u64) {
    wasmdb_debug::bump_notification_count(sub_id);
}

pub(crate) fn track_table_invalidations(zset: &ZSet) {
    wasmdb_debug::track_table_invalidations(zset);
}

pub(crate) fn record_query(sql: &str, source: &str, spans: Vec<Span>, row_count: usize) {
    wasmdb_debug::record_query(now_ms(), sql, source, spans, row_count);
}

// ── WASM exports — pure readers ─────────────────────────────────────

#[wasm_bindgen]
pub fn debug_event_log() -> Result<JsValue, JsError> {
    serde_wasm_bindgen::to_value(&wasmdb_debug::snapshot_events())
        .map_err(|e| JsError::new(&e.to_string()))
}

#[wasm_bindgen]
pub fn debug_event_count() -> u64 {
    wasmdb_debug::event_count()
}

#[wasm_bindgen]
pub fn debug_clear_log() {
    wasmdb_debug::clear();
}

#[wasm_bindgen]
pub fn debug_query_log() -> Result<JsValue, JsError> {
    serde_wasm_bindgen::to_value(&wasmdb_debug::snapshot_queries())
        .map_err(|e| JsError::new(&e.to_string()))
}

#[wasm_bindgen]
pub fn debug_query_stats() -> Result<JsValue, JsError> {
    #[derive(Serialize)]
    struct QueryStats {
        total_queries: u64,
        slow_queries: u64,
        table_invalidation_counts: HashMap<String, u64>,
    }

    let (total, slow) = wasmdb_debug::query_totals();
    let stats = QueryStats {
        total_queries: total,
        slow_queries: slow,
        table_invalidation_counts: wasmdb_debug::snapshot_table_invalidations(),
    };
    serde_wasm_bindgen::to_value(&stats)
        .map_err(|e| JsError::new(&e.to_string()))
}

#[wasm_bindgen]
pub fn debug_wasm_memory() -> usize {
    let mem = wasm_bindgen::memory();
    let buf = js_sys::Reflect::get(&mem, &"buffer".into()).unwrap_or(JsValue::NULL);
    let len = js_sys::Reflect::get(&buf, &"byteLength".into()).unwrap_or(JsValue::from(0));
    len.as_f64().unwrap_or(0.0) as usize
}

// ── WASM exports — client-dependent (Command-coupled boundary) ──────

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

        let notification_counts = wasmdb_debug::snapshot_notification_counts();

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
