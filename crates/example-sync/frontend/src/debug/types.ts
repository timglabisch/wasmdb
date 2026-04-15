export type DebugEvent =
  | { kind: 'Execute'; timestamp_ms: number; stream_id: number; command_json: string; zset_entry_count: number }
  | { kind: 'FetchStart'; timestamp_ms: number; stream_id: number; request_bytes: number }
  | { kind: 'FetchEnd'; timestamp_ms: number; stream_id: number; response_bytes: number; latency_ms: number }
  | { kind: 'Confirmed'; timestamp_ms: number; stream_id: number }
  | { kind: 'Rejected'; timestamp_ms: number; stream_id: number; reason: string }
  | { kind: 'Notification'; timestamp_ms: number; affected_sub_ids: number[]; total_subs: number }
  | { kind: 'SubscriptionCreated'; timestamp_ms: number; sub_id: number; sql: string; tables: string[] }
  | { kind: 'SubscriptionRemoved'; timestamp_ms: number; sub_id: number };

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
  tables: string[];
}

export interface SubscriptionDebug {
  count: number;
  subscriptions: SubInfo[];
  notification_counts: Record<number, number>;
  reverse_index_size: number;
}

export interface ColumnInfo {
  name: string;
  data_type: string;
  nullable: boolean;
}

export interface TableInfo {
  name: string;
  row_count: number;
  physical_len: number;
  columns: ColumnInfo[];
  index_count: number;
}

export interface DbInfo {
  tables: TableInfo[];
}

export interface DatabaseDebug {
  optimistic: DbInfo;
  confirmed: DbInfo;
}

export interface DebugSnapshot {
  syncStatus: SyncStatus;
  subscriptions: SubscriptionDebug;
  database: DatabaseDebug;
  events: DebugEvent[];
  wasmMemoryBytes: number;
  totalEventCount: number;
  timestamp: number;
}
