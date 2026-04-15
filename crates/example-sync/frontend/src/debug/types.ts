export type DebugEvent =
  | { kind: 'Execute'; timestamp_ms: number; stream_id: number; command_json: string; zset_entry_count: number }
  | { kind: 'FetchStart'; timestamp_ms: number; stream_id: number; request_bytes: number }
  | { kind: 'FetchEnd'; timestamp_ms: number; stream_id: number; response_bytes: number; latency_ms: number }
  | { kind: 'Confirmed'; timestamp_ms: number; stream_id: number }
  | { kind: 'Rejected'; timestamp_ms: number; stream_id: number; reason: string }
  | { kind: 'Notification'; timestamp_ms: number; affected_sub_ids: number[]; total_subs: number }
  | { kind: 'SubscriptionCreated'; timestamp_ms: number; sub_id: number; sql: string; tables: string[] }
  | { kind: 'SubscriptionRemoved'; timestamp_ms: number; sub_id: number }
  | { kind: 'QueryExecuted'; timestamp_ms: number; sql: string; duration_us: number; row_count: number; source: string }
  | { kind: 'SlowQuery'; timestamp_ms: number; sql: string; duration_us: number };

export interface PendingDetail {
  seq_no: number;
  zset_entries: number;
}

export interface StreamInfo {
  id: number;
  pending_count: number;
  is_idle: boolean;
  pending: PendingDetail[];
}

export interface SyncStatus {
  stream_count: number;
  total_pending: number;
  streams: StreamInfo[];
}

export interface SubInfo {
  id: number;
  sql: string;
  tables: string[];
  notification_count: number;
}

export interface SubscriptionDebug {
  count: number;
  subscriptions: SubInfo[];
  reverse_index_size: number;
}

export interface ColumnInfo {
  name: string;
  data_type: string;
  nullable: boolean;
}

export interface IndexInfo {
  columns: string[];
  index_type: 'BTree' | 'Hash';
  key_count: number;
}

export interface TableInfo {
  name: string;
  row_count: number;
  physical_len: number;
  deleted_count: number;
  fragmentation_ratio: number;
  columns: ColumnInfo[];
  index_count: number;
  indexes: IndexInfo[];
  estimated_memory_bytes: number;
}

export interface DbInfo {
  tables: TableInfo[];
}

export interface DatabaseDebug {
  optimistic: DbInfo;
  confirmed: DbInfo;
}

// ── Query execution traces ────────────────────────────────────────

export type ScanMethodInfo =
  | 'Full'
  | { Index: { columns: number[]; prefix_len: number; is_hash: boolean } };

// Serde externally-tagged: unit variants are strings, struct variants are { Name: {...} }
export type SpanOperationInfo =
  | 'Execute'
  | { Materialize: { step: number } }
  | { Scan: { table: string; method: ScanMethodInfo; rows: number } }
  | { Filter: { rows_in: number; rows_out: number } }
  | { Join: { rows_out: number } }
  | { Aggregate: { groups: number } }
  | { Sort: { rows: number } }
  | { Project: { columns: number; rows: number } };

export interface SpanInfo {
  operation: SpanOperationInfo;
  duration_us: number;
  children: SpanInfo[];
}

export interface QueryTrace {
  timestamp_ms: number;
  sql: string;
  duration_us: number;
  row_count: number;
  source: 'optimistic' | 'confirmed';
  spans: SpanInfo[];
  is_slow: boolean;
}

export interface QueryStats {
  total_queries: number;
  slow_queries: number;
  table_invalidation_counts: Record<string, number>;
}

// ── Aggregated snapshot ───────────────────────────────────────────

export interface DebugSnapshot {
  syncStatus: SyncStatus;
  subscriptions: SubscriptionDebug;
  database: DatabaseDebug;
  events: DebugEvent[];
  wasmMemoryBytes: number;
  totalEventCount: number;
  queryLog: QueryTrace[];
  queryStats: QueryStats;
  timestamp: number;
}
