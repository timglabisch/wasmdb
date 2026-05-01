import type { SyncStatus, SubscriptionDebug, DatabaseDebug, DebugEvent, DebugSnapshot, QueryTrace, QueryStats } from './types';

export interface WasmDebugApi {
  debug_sync_status(): SyncStatus;
  debug_subscriptions(): SubscriptionDebug;
  debug_database(): DatabaseDebug;
  debug_event_log(): DebugEvent[];
  debug_table_rows(tableName: string, dbKind: string, limit: number): any[][];
  debug_wasm_memory(): number;
  debug_event_count(): bigint | number;
  debug_clear_log(): void;
  debug_query_log?(): QueryTrace[];
  debug_query_stats?(): QueryStats;
}

let wasmRef: WasmDebugApi | null = null;

/** Call once at application boot with the wasm module (or any object exposing the
 * `debug_*` functions). Without this, the toolbar APIs throw. */
export function setDebugWasm(wasm: WasmDebugApi): void {
  wasmRef = wasm;
}

function wasm(): WasmDebugApi {
  if (!wasmRef) throw new Error('debug-toolbar: call setDebugWasm(wasm) before use');
  return wasmRef;
}

export function getSyncStatus(): SyncStatus {
  return wasm().debug_sync_status();
}

export function getSubscriptions(): SubscriptionDebug {
  return wasm().debug_subscriptions();
}

export function getDatabase(): DatabaseDebug {
  return wasm().debug_database();
}

export function getEventLog(): DebugEvent[] {
  return wasm().debug_event_log();
}

export function getTableRows(tableName: string, dbKind: string, limit: number = 100): any[][] {
  return wasm().debug_table_rows(tableName, dbKind, limit);
}

export function getWasmMemory(): number {
  return wasm().debug_wasm_memory();
}

export function getEventCount(): number {
  return Number(wasm().debug_event_count());
}

export function clearLog(): void {
  wasm().debug_clear_log();
}

const EMPTY_QUERY_STATS: QueryStats = { total_queries: 0, slow_queries: 0, table_invalidation_counts: {} };

export function getQueryLog(): QueryTrace[] {
  const w = wasm();
  if (typeof w.debug_query_log !== 'function') return [];
  return w.debug_query_log();
}

export function getQueryStats(): QueryStats {
  const w = wasm();
  if (typeof w.debug_query_stats !== 'function') return EMPTY_QUERY_STATS;
  return w.debug_query_stats();
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
