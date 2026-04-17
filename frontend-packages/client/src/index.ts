import { useState, useEffect, useRef } from 'react';

// ── Types ─────────────────────────────────────────────────────────

export interface Execution {
  zset: unknown;
  confirmed: Promise<{ status: 'confirmed' | 'rejected'; reason?: string }>;
}

/** Surface of the wasm-bindgen module needed by this client library. */
export interface WasmSyncApi {
  execute(cmdJson: string): Execution;
  execute_on_stream(streamId: number, cmdJson: string): Execution;
  create_stream(batchCount: number, batchWaitMs: number, retryCount: number): number;
  flush_stream(streamId: number): Promise<void>;
  query(sql: string): any[][];
  query_confirmed(sql: string): any[][];
  /**
   * Register a reactive subscription. Returns `{handle, subId}`:
   * - `handle` is unique per call and is what you pass back to `unsubscribe`.
   * - `subId` is the shared runtime id — multiple calls with equivalent SQL
   *   resolve to the same `subId`. Useful as a cache key for stores that want
   *   to dedupe per-query state across components.
   */
  subscribe(sql: string, callback: Function): { handle: number; subId: number };
  /** Release a caller handle. Unknown handles log a console warning. */
  unsubscribe(handle: number): void;
  next_id(): number;
}

// ── Module-level wasm ref + ready-state ───────────────────────────

let wasmRef: WasmSyncApi | null = null;
let wasmReady = false;
const readyListeners = new Set<() => void>();
let bootstrapping = false;

/** Inject the wasm module. Call once after the bootstrap (wasm init) resolves. */
export function provideWasm(wasm: WasmSyncApi): void {
  wasmRef = wasm;
}

/** Mark the wasm boot as finished and wake `useWasm` subscribers. */
export function markReady(): void {
  wasmReady = true;
  readyListeners.forEach(fn => fn());
  readyListeners.clear();
}

export function isReady(): boolean {
  return wasmReady;
}

function wasm(): WasmSyncApi {
  if (!wasmRef) throw new Error('@wasmdb/client: call provideWasm(wasm) before use');
  return wasmRef;
}

// ── Standalone wrappers ───────────────────────────────────────────

export function execute<C = unknown>(cmd: C): Execution {
  return wasm().execute(JSON.stringify(cmd));
}

export function executeOnStream<C = unknown>(streamId: number, cmd: C): Execution {
  return wasm().execute_on_stream(streamId, JSON.stringify(cmd));
}

export function createStream(batchCount: number = 1, batchWaitMs: number = 0, retryCount: number = 0): number {
  return wasm().create_stream(batchCount, batchWaitMs, retryCount);
}

export function flushStream(streamId: number): Promise<void> {
  return wasm().flush_stream(streamId);
}

export function nextId(): number {
  return wasm().next_id();
}

// ── React hooks ───────────────────────────────────────────────────

/**
 * Boot wasm once per process. Pass an async `bootstrap` that: loads the wasm
 * module, runs its `init()`, and calls `provideWasm(...)`. On the first mount
 * this runs the bootstrap; subsequent mounts short-circuit.
 */
export function useWasm(bootstrap: () => Promise<void>): boolean {
  const [ready, setReady] = useState(wasmReady);

  useEffect(() => {
    if (wasmReady) {
      setReady(true);
      return;
    }
    const listener = () => setReady(true);
    readyListeners.add(listener);

    if (!bootstrapping) {
      bootstrapping = true;
      bootstrap().then(markReady);
    }

    return () => { readyListeners.delete(listener); };
  }, []);

  return ready;
}

function useReactiveQuery<T>(
  sql: string,
  dbKind: 'optimistic' | 'confirmed',
  mapRow?: (row: any[]) => T,
): T[] {
  const [data, setData] = useState<T[]>([]);
  const mapRef = useRef(mapRow);
  mapRef.current = mapRow;

  useEffect(() => {
    if (!wasmReady) return;
    const w = wasm();
    const read = () => dbKind === 'confirmed' ? w.query_confirmed(sql) : w.query(sql);

    const refresh = () => {
      const rows = read();
      setData(mapRef.current ? rows.map(mapRef.current) : (rows as T[]));
    };

    const { handle } = w.subscribe(sql, refresh);
    refresh();

    return () => { w.unsubscribe(handle); };
  }, [sql, dbKind]);

  return data;
}

export function useQuery<T = any>(sql: string, mapRow?: (row: any[]) => T): T[] {
  return useReactiveQuery(sql, 'optimistic', mapRow);
}

export function useQueryConfirmed<T = any>(sql: string, mapRow?: (row: any[]) => T): T[] {
  return useReactiveQuery(sql, 'confirmed', mapRow);
}
