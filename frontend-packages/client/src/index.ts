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

/**
 * A named-parameter map for prepared-statement-style queries. Placeholders
 * in SQL are written as `:name` (e.g. `WHERE id = :id`). Accepted value
 * types map 1:1 to the engine's `ParamValue` enum: integer numbers,
 * strings, `null`, integer arrays and string arrays.
 *
 * Floats/booleans are rejected — the engine has no corresponding variants
 * today. Pass an empty object or omit the argument for a query without
 * parameters.
 */
export type QueryParams = Record<string, number | string | null | number[] | string[]>;

/** Surface of the wasm-bindgen module needed by this client library. */
export interface WasmSyncApi {
  execute(cmdJson: string): Execution;
  execute_on_stream(streamId: number, cmdJson: string): Execution;
  create_stream(batchCount: number, batchWaitMs: number, retryCount: number): number;
  flush_stream(streamId: number): Promise<void>;
  query(sql: string, params?: QueryParams | null): any[][];
  /**
   * Async sibling of `query`. Required when the SQL contains a
   * `schema.fn(args)` source — the sync path refuses those because it
   * would have to await an HTTP roundtrip. Optional in the shape because
   * not every demo wires it up (sync-demo doesn't need fetchers).
   */
  query_async?(sql: string, params?: QueryParams | null): Promise<any[][]>;
  /**
   * `triggered` is a `number[]` (e.g. from `DirtyNotification.triggered`).
   * Pass `undefined` or `[]` for a cold read without REACTIVE(...) highlighting.
   */
  query_confirmed(sql: string, triggered?: number[], params?: QueryParams | null): any[][];
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

  // ── Requirements API (typed-deps useQuery overload) ─────────────
  /**
   * Subscribe to a `(sql, requires)` derived requirement. `requires_json`
   * is `[{id, args}]`. The callback fires whenever the derived slot or any
   * of its upstream Fetched slots change state.
   */
  requirements_subscribe?(
    sql: string,
    requires_json: string,
    on_change: () => void,
  ): number;
  requirements_unsubscribe?(sub: number): void;
  requirements_invalidate?(key: string): void;
  requirements_status?(sub: number): { state: RequirementState; error?: string };
}

export type RequirementState = 'idle' | 'loading' | 'ready' | 'error';

export interface RequirementSpec {
  id: string;
  args: (number | string | null)[];
}

export interface QueryWithRequires {
  sql: string;
  requires: RequirementSpec[];
}

/** Result of `useQuery({sql, requires})`. */
export interface UseQueryResult<T> {
  data: T[];
  status: RequirementState;
  error?: string;
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

/**
 * Generate a fresh UUIDv4 as the canonical hyphenated lowercase string.
 *
 * Uses the browser-native `crypto.randomUUID()` (no wasm round-trip) — the
 * Rust side mirrors this with `Uuid::new_v4()` for server/shared paths.
 * Both flow into the same `BINARY(16)` storage; ts-rs serializes the
 * shared `Uuid` newtype as a string so command JSON wires through cleanly.
 */
export function nextId(): string {
  return crypto.randomUUID();
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
  params?: QueryParams,
): T[] {
  const [data, setData] = useState<T[]>([]);
  const mapRef = useRef(mapRow);
  mapRef.current = mapRow;
  // Stringify once per render so the effect re-subscribes when any param
  // changes, without requiring callers to memoize the params object.
  const paramsKey = params ? JSON.stringify(params) : '';

  useEffect(() => {
    if (!wasmReady) return;
    const w = wasm();
    const { handle, subId } = w.subscribe(sql);
    const boundParams = paramsKey ? JSON.parse(paramsKey) as QueryParams : undefined;

    const read = () => {
      if (dbKind === 'confirmed') {
        const triggered = lastTriggeredBySubId.get(subId);
        return w.query_confirmed(sql, triggered, boundParams);
      }
      return w.query(sql, boundParams);
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
  }, [sql, dbKind, paramsKey]);

  return data;
}

export function useQuery<T = any>(
  sql: string,
  mapRow?: (row: any[]) => T,
  params?: QueryParams,
): T[];
export function useQuery<T = any>(
  spec: QueryWithRequires,
  mapRow?: (row: any[]) => T,
  params?: QueryParams,
): UseQueryResult<T>;
export function useQuery<T = any>(
  arg: string | QueryWithRequires,
  mapRow?: (row: any[]) => T,
  params?: QueryParams,
): T[] | UseQueryResult<T> {
  if (typeof arg === 'string') {
    return useReactiveQuery(arg, 'optimistic', mapRow, params);
  }
  return useRequirementsQuery(arg, mapRow, params);
}

function useRequirementsQuery<T>(
  spec: QueryWithRequires,
  mapRow?: (row: any[]) => T,
  params?: QueryParams,
): UseQueryResult<T> {
  const [data, setData] = useState<T[]>([]);
  const [status, setStatus] = useState<RequirementState>('loading');
  const [error, setError] = useState<string | undefined>(undefined);
  const mapRef = useRef(mapRow);
  mapRef.current = mapRow;

  const requiresKey = JSON.stringify(spec.requires);
  const paramsKey = params ? JSON.stringify(params) : '';
  const { sql } = spec;

  useEffect(() => {
    if (!wasmReady) return;
    const w = wasm();
    if (!w.requirements_subscribe || !w.requirements_unsubscribe || !w.requirements_status) {
      throw new Error(
        '@wasmdb/client: useQuery({sql, requires}) requires the wasm module to expose `requirements_subscribe`, `requirements_unsubscribe`, and `requirements_status`',
      );
    }
    let cancelled = false;
    const boundParams = paramsKey ? (JSON.parse(paramsKey) as QueryParams) : undefined;

    const refresh = (subId: number) => {
      const s = w.requirements_status!(subId);
      if (cancelled) return;
      setStatus(s.state);
      setError(s.error);
      if (s.state === 'ready') {
        try {
          const rows = w.query(sql, boundParams);
          if (cancelled) return;
          setData(mapRef.current ? rows.map(mapRef.current) : (rows as T[]));
        } catch (e) {
          if (cancelled) return;
          setStatus('error');
          setError(String(e));
        }
      }
    };

    const subId = w.requirements_subscribe!(sql, requiresKey, () => {
      refresh(subId);
    });
    refresh(subId);

    return () => {
      cancelled = true;
      w.requirements_unsubscribe!(subId);
    };
  }, [sql, requiresKey, paramsKey]);

  return { data, status, error };
}

/**
 * Declare a set of requirements without running a SQL query.
 *
 * Use this on a top-level page that has many sub-components, each running
 * their own `useQuery(sql, mapRow)`. The page gates rendering on `status`,
 * sub-components stay simple. Re-runs subscribe whenever the `requires`
 * shape changes.
 */
export function useRequirements(
  requires: RequirementSpec[],
): { status: RequirementState; error?: string } {
  const [status, setStatus] = useState<RequirementState>('loading');
  const [error, setError] = useState<string | undefined>(undefined);

  const requiresKey = JSON.stringify(requires);

  useEffect(() => {
    if (!wasmReady) return;
    const w = wasm();
    if (!w.requirements_subscribe || !w.requirements_unsubscribe || !w.requirements_status) {
      throw new Error(
        '@wasmdb/client: useRequirements requires the wasm module to expose `requirements_subscribe`, `requirements_unsubscribe`, and `requirements_status`',
      );
    }
    let cancelled = false;

    const refresh = (subId: number) => {
      const s = w.requirements_status!(subId);
      if (cancelled) return;
      setStatus(s.state);
      setError(s.error);
    };

    const subId = w.requirements_subscribe!('__requirements_only__', requiresKey, () => {
      refresh(subId);
    });
    refresh(subId);

    return () => {
      cancelled = true;
      w.requirements_unsubscribe!(subId);
    };
  }, [requiresKey]);

  return { status, error };
}

export function useQueryConfirmed<T = any>(
  sql: string,
  mapRow?: (row: any[]) => T,
  params?: QueryParams,
): T[] {
  return useReactiveQuery(sql, 'confirmed', mapRow, params);
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
export function useAsyncQuery<T = any>(
  sql: string,
  mapRow?: (row: any[]) => T,
  params?: QueryParams,
): T[] {
  const [data, setData] = useState<T[]>([]);
  const mapRef = useRef(mapRow);
  mapRef.current = mapRow;
  const paramsKey = params ? JSON.stringify(params) : '';

  useEffect(() => {
    if (!wasmReady) return;
    const w = wasm();
    if (!w.query_async) {
      throw new Error('@wasmdb/client: useAsyncQuery requires the wasm module to expose `query_async`');
    }
    const { handle, subId } = w.subscribe(sql);
    const boundParams = paramsKey ? JSON.parse(paramsKey) as QueryParams : undefined;

    let cancelled = false;
    const refresh = async () => {
      try {
        const rows = await w.query_async!(sql, boundParams);
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
  }, [sql, paramsKey]);

  return data;
}
