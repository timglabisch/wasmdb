import * as wasm from '../../wasm-pkg/example_sync_wasm';
import type { SyncStatus, SubscriptionDebug, DatabaseDebug, DebugEvent, DebugSnapshot, QueryTrace, QueryStats } from './types';

export function getSyncStatus(): SyncStatus {
  return wasm.debug_sync_status();
}

export function getSubscriptions(): SubscriptionDebug {
  return wasm.debug_subscriptions();
}

export function getDatabase(): DatabaseDebug {
  return wasm.debug_database();
}

export function getEventLog(): DebugEvent[] {
  return wasm.debug_event_log();
}

export function getTableRows(tableName: string, dbKind: 'optimistic' | 'confirmed', limit: number = 100): any[][] {
  return wasm.debug_table_rows(tableName, dbKind, limit);
}

export function getWasmMemory(): number {
  return wasm.debug_wasm_memory();
}

export function getEventCount(): number {
  return Number(wasm.debug_event_count());
}

export function clearLog(): void {
  wasm.debug_clear_log();
}

const EMPTY_QUERY_STATS: QueryStats = { total_queries: 0, slow_queries: 0, table_invalidation_counts: {} };

export function getQueryLog(): QueryTrace[] {
  if (typeof wasm.debug_query_log !== 'function') return [];
  return wasm.debug_query_log();
}

export function getQueryStats(): QueryStats {
  if (typeof wasm.debug_query_stats !== 'function') return EMPTY_QUERY_STATS;
  return wasm.debug_query_stats();
}

export function getDebugSnapshot(): DebugSnapshot {
  return {
    syncStatus: getSyncStatus(),
    subscriptions: getSubscriptions(),
    database: getDatabase(),
    events: getEventLog(),
    wasmMemoryBytes: getWasmMemory(),
    totalEventCount: getEventCount(),
    queryLog: getQueryLog(),
    queryStats: getQueryStats(),
    timestamp: Date.now(),
  };
}
