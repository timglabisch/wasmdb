import {
  debug_event_log,
  debug_sync_status,
  debug_subscriptions,
  debug_database,
  debug_table_rows,
  debug_wasm_memory,
  debug_event_count,
  debug_clear_log,
} from '../../wasm-pkg/example_sync_wasm';
import type { SyncStatus, SubscriptionDebug, DatabaseDebug, DebugEvent, DebugSnapshot } from './types';

export function getSyncStatus(): SyncStatus {
  return debug_sync_status();
}

export function getSubscriptions(): SubscriptionDebug {
  return debug_subscriptions();
}

export function getDatabase(): DatabaseDebug {
  return debug_database();
}

export function getEventLog(): DebugEvent[] {
  return debug_event_log();
}

export function getTableRows(tableName: string, dbKind: 'optimistic' | 'confirmed', limit: number = 100): any[][] {
  return debug_table_rows(tableName, dbKind, limit);
}

export function getWasmMemory(): number {
  return debug_wasm_memory();
}

export function getEventCount(): number {
  return debug_event_count();
}

export function clearLog(): void {
  debug_clear_log();
}

export function getDebugSnapshot(): DebugSnapshot {
  return {
    syncStatus: getSyncStatus(),
    subscriptions: getSubscriptions(),
    database: getDatabase(),
    events: getEventLog(),
    wasmMemoryBytes: getWasmMemory(),
    totalEventCount: getEventCount(),
    timestamp: Date.now(),
  };
}
