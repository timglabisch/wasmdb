import { useState, useEffect, useRef } from 'react';

// ── Types ─────────────────────────────────────────────────────────

export interface Execution {
  zset: unknown;
  confirmed: Promise<{ status: 'confirmed' | 'rejected'; reason?: string }>;
}

/** One drain item from `next_dirty()`. */
export interface DirtyNotification {
  subId: number;
  /** Triggered reactive condition indices, accumulated since the last drain. */
  triggered: number[];
}

/** Surface of the wasm-bindgen module needed by this client library. */
export interface WasmSyncApi {
  execute(cmdJson: string): Execution;
  execute_on_stream(streamId: number, cmdJson: string): Execution;
  create_stream(batchCount: number, batchWaitMs: number, retryCount: number): number;
  flush_stream(streamId: number): Promise<void>;
  query(sql: string): any[][];
  /**
   * Async sibling of `query`. Required when the SQL contains a
   * `schema.fn(args)` source — the sync path refuses those because it
   * would have to await an HTTP roundtrip. Optional in the shape because
   * not every demo wires it up (sync-demo doesn't need fetchers).
   */
  query_async?(sql: string): Promise<any[][]>;
  /**
   * `triggered` is a `number[]` (e.g. from `DirtyNotification.triggered`).
   * Pass `undefined` or `[]` for a cold read without REACTIVE(...) highlighting.
   */
  query_confirmed(sql: string, triggered?: number[]): any[][];
  /**
   * Register a reactive subscription. Returns `{handle, subId}`:
   * - `handle` is unique per call and is what you pass back to `unsubscribe`.
   * - `subId` is the shared runtime id — multiple calls with equivalent SQL
   *   resolve to the same `subId`. Useful as a cache key for stores that want
   *   to dedupe per-query state across components.
   */
  subscribe(sql: string): { handle: number; subId: number };
  /** Release a caller handle. Unknown handles log a console warning. */
  unsubscribe(handle: number): void;
  /**
   * Register a single edge-triggered wake callback. Fires once when the
   * internal dirty-set transitions from empty to non-empty. Use this to
   * schedule a drain (e.g. via `queueMicrotask`).
   */
  on_dirty(wake: () => void): void;
  /**
   * Pull the next dirty notification, or `null` when this drain cycle is
   * exhausted. Call in a loop until it returns `null` to finish the cycle.
   */
  next_dirty(): DirtyNotification | null;
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
  installDrainPump(wasm);
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

// ── Drain pump: wasm `on_dirty` → scheduled drain → per-sub listeners ──

/** Per-subscription refresh handlers, keyed by shared runtime subId. */
const listenersBySubId = new Map<number, Set<() => void>>();
/** handle → (subId, refreshFn) so we can remove from listenersBySubId on unsubscribe. */
const handleIndex = new Map<number, { subId: number; fn: () => void }>();
/** Most recent triggered indices per subId — read by `useReactiveQuery` when
 *  querying the confirmed side with REACTIVE(...) columns. */
const lastTriggeredBySubId = new Map<number, number[]>();

let drainScheduled = false;

function installDrainPump(w: WasmSyncApi): void {
  w.on_dirty(() => {
    if (drainScheduled) return;
    drainScheduled = true;
    queueMicrotask(() => {
      drainScheduled = false;
      drainPending();
    });
  });
}

function drainPending(): void {
  if (!wasmRef) return;
  while (true) {
    const n = wasmRef.next_dirty();
    if (n === null) break;
    lastTriggeredBySubId.set(n.subId, n.triggered);
    const listeners = listenersBySubId.get(n.subId);
    if (!listeners) continue;
    listeners.forEach(fn => fn());
  }
}

function addListener(subId: number, handle: number, fn: () => void): void {
  let set = listenersBySubId.get(subId);
  if (!set) {
    set = new Set();
    listenersBySubId.set(subId, set);
  }
  set.add(fn);
  handleIndex.set(handle, { subId, fn });
}

function removeListener(handle: number): void {
  const entry = handleIndex.get(handle);
  if (!entry) return;
  handleIndex.delete(handle);
  const set = listenersBySubId.get(entry.subId);
  if (!set) return;
  set.delete(entry.fn);
  if (set.size === 0) {
    listenersBySubId.delete(entry.subId);
    lastTriggeredBySubId.delete(entry.subId);
  }
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
    const { handle, subId } = w.subscribe(sql);

    const read = () => {
      if (dbKind === 'confirmed') {
        const triggered = lastTriggeredBySubId.get(subId);
        return w.query_confirmed(sql, triggered);
      }
      return w.query(sql);
    };

    const refresh = () => {
      const rows = read();
      setData(mapRef.current ? rows.map(mapRef.current) : (rows as T[]));
    };

    addListener(subId, handle, refresh);
    refresh();

    return () => {
      removeListener(handle);
      w.unsubscribe(handle);
    };
  }, [sql, dbKind]);

  return data;
}

export function useQuery<T = any>(sql: string, mapRow?: (row: any[]) => T): T[] {
  return useReactiveQuery(sql, 'optimistic', mapRow);
}

export function useQueryConfirmed<T = any>(sql: string, mapRow?: (row: any[]) => T): T[] {
  return useReactiveQuery(sql, 'confirmed', mapRow);
}

/**
 * Like `useQuery`, but drives the read through the async wasm path
 * (`query_async`). Use whenever the SQL contains a `schema.fn(args)`
 * source — those hit the server via `/table-fetch` during Phase 0 and
 * the sync `query` would bail with `RequiresAsync`.
 *
 * Rendering semantics: first render returns `[]`, then re-renders once
 * the async read resolves. Subscription-driven refreshes also go through
 * the same async read.
 */
export function useAsyncQuery<T = any>(sql: string, mapRow?: (row: any[]) => T): T[] {
  const [data, setData] = useState<T[]>([]);
  const mapRef = useRef(mapRow);
  mapRef.current = mapRow;

  useEffect(() => {
    if (!wasmReady) return;
    const w = wasm();
    if (!w.query_async) {
      throw new Error('@wasmdb/client: useAsyncQuery requires the wasm module to expose `query_async`');
    }
    const { handle, subId } = w.subscribe(sql);

    let cancelled = false;
    const refresh = async () => {
      try {
        const rows = await w.query_async!(sql);
        if (cancelled) return;
        setData(mapRef.current ? rows.map(mapRef.current) : (rows as T[]));
      } catch (e) {
        if (cancelled) return;
        // eslint-disable-next-line no-console
        console.error('[useAsyncQuery] query_async failed', { sql, error: e });
      }
    };

    addListener(subId, handle, () => { void refresh(); });
    void refresh();

    return () => {
      cancelled = true;
      removeListener(handle);
      w.unsubscribe(handle);
    };
  }, [sql]);

  return data;
}
