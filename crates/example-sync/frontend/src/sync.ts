import { useState, useEffect, useCallback } from 'react';
import initWasm, {
  init,
  execute as wasmExecute,
  query_users,
  set_on_change,
  next_id,
} from '../wasm-pkg/example_sync_wasm';
import type { UserCommand } from './generated/UserCommand';

// ── Types ─────────────────────────────────────────────────────────

export type { UserCommand };

export type SyncStatus = 'pending' | 'confirmed';

export interface User {
  id: number;
  name: string;
  age: number;
  sync: SyncStatus;
}

export interface Execution {
  zset: unknown;
  confirmed: Promise<{ status: 'confirmed' | 'rejected'; reason?: string }>;
}

// ── Standalone functions (no React dependency) ───────────────────

export function execute(cmd: UserCommand): Execution {
  return wasmExecute(JSON.stringify(cmd));
}

export { next_id as nextId };

// ── Hook (reactive state only) ───────────────────────────────────

let wasmReady = false;

export function useSync() {
  const [ready, setReady] = useState(false);
  const [users, setUsers] = useState<User[]>([]);

  const refresh = useCallback(() => {
    if (wasmReady) setUsers(query_users());
  }, []);

  useEffect(() => {
    initWasm().then(() => {
      init();
      wasmReady = true;
      set_on_change(refresh);
      setReady(true);
      refresh();
    });
  }, [refresh]);

  return { ready, users };
}
