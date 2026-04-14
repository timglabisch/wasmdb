import { useState, useEffect, useRef } from 'react';
import initWasm, {
  init,
  execute as wasmExecute,
  query as wasmQuery,
  query_confirmed as wasmQueryConfirmed,
  set_on_change,
  next_id,
} from '../wasm-pkg/example_sync_wasm';
import type { UserCommand } from './generated/UserCommand';

// ── Types ─────────────────────────────────────────────────────────

export type { UserCommand };

export interface Execution {
  zset: unknown;
  confirmed: Promise<{ status: 'confirmed' | 'rejected'; reason?: string }>;
}

// ── Subscriber pattern ────────────────────────────────────────────

let wasmReady = false;
const listeners = new Set<() => void>();

function notifyAll() {
  listeners.forEach(fn => fn());
}

// ── Standalone functions (no React dependency) ───────────────────

export function execute(cmd: UserCommand): Execution {
  return wasmExecute(JSON.stringify(cmd));
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
    initWasm().then(() => {
      init();
      wasmReady = true;
      set_on_change(notifyAll);
      setReady(true);
      notifyAll();
    });
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
    const refresh = () => {
      if (!wasmReady) return;
      const rows: any[] = queryFn(sql);
      setData(mapRef.current ? rows.map(mapRef.current) : rows);
    };
    refresh();
    listeners.add(refresh);
    return () => { listeners.delete(refresh); };
  }, [sql, queryFn]);

  return data;
}

export function useQuery<T = any>(sql: string, mapRow?: (row: any[]) => T): T[] {
  return useQueryInternal(wasmQuery, sql, mapRow);
}

export function useQueryConfirmed<T = any>(sql: string, mapRow?: (row: any[]) => T): T[] {
  return useQueryInternal(wasmQueryConfirmed, sql, mapRow);
}
