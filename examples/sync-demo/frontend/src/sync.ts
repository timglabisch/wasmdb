import { useState, useEffect, useRef } from 'react';
import initWasm, * as wasm from '../wasm-pkg/sync_demo_wasm';
import {
  init,
  execute as wasmExecute,
  execute_on_stream as wasmExecuteOnStream,
  create_stream as wasmCreateStream,
  flush_stream as wasmFlushStream,
  query as wasmQuery,
  query_confirmed as wasmQueryConfirmed,
  subscribe as wasmSubscribe,
  unsubscribe as wasmUnsubscribe,
  next_id,
} from '../wasm-pkg/sync_demo_wasm';
import { setDebugWasm } from '@wasmdb/debug-toolbar';
import type { UserCommand } from './generated/UserCommand';

// ── Types ─────────────────────────────────────────────────────────

export type { UserCommand };

export interface Execution {
  zset: unknown;
  confirmed: Promise<{ status: 'confirmed' | 'rejected'; reason?: string }>;
}

// ── WASM readiness ────────────────────────────────────────────────

let wasmReady = false;
const readyListeners = new Set<() => void>();

// ── Standalone functions (no React dependency) ───────────────────

export function execute(cmd: UserCommand): Execution {
  return wasmExecute(JSON.stringify(cmd));
}

export function createStream(batchCount: number = 1, batchWaitMs: number = 0, retryCount: number = 0): number {
  return wasmCreateStream(batchCount, batchWaitMs, retryCount);
}

export function executeOnStream(streamId: number, cmd: UserCommand): Execution {
  return wasmExecuteOnStream(streamId, JSON.stringify(cmd));
}

export function flushStream(streamId: number): Promise<void> {
  return wasmFlushStream(streamId);
}

export { next_id as nextId };

// ── Hooks ─────────────────────────────────────────────────────────

export function useWasm(): boolean {
  const [ready, setReady] = useState(wasmReady);

  useEffect(() => {
    if (wasmReady) {
      setReady(true);
      return;
    }
    const listener = () => setReady(true);
    readyListeners.add(listener);

    initWasm().then(() => {
      init();
      setDebugWasm(wasm as any);
      wasmReady = true;
      readyListeners.forEach(fn => fn());
      readyListeners.clear();
    });

    return () => { readyListeners.delete(listener); };
  }, []);

  return ready;
}

function useQueryInternal<T>(
  queryFn: (sql: string) => any,
  sql: string,
  mapRow?: (row: any[]) => T,
): T[] {
  const [data, setData] = useState<T[]>([]);
  const mapRef = useRef(mapRow);
  mapRef.current = mapRow;

  useEffect(() => {
    if (!wasmReady) return;

    const refresh = () => {
      const rows: any[] = queryFn(sql);
      setData(mapRef.current ? rows.map(mapRef.current) : rows);
    };

    // Subscribe — WASM calls refresh when affected tables change.
    const subId = wasmSubscribe(sql, refresh);
    refresh(); // initial fetch

    return () => { wasmUnsubscribe(subId); };
  }, [sql, queryFn]);

  return data;
}

export function useQuery<T = any>(sql: string, mapRow?: (row: any[]) => T): T[] {
  return useQueryInternal(wasmQuery, sql, mapRow);
}

export function useQueryConfirmed<T = any>(sql: string, mapRow?: (row: any[]) => T): T[] {
  return useQueryInternal(wasmQueryConfirmed, sql, mapRow);
}
