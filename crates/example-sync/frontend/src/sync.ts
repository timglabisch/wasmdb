import { useState, useEffect, useCallback, useRef } from 'react';
import initWasm, {
  init,
  create_stream,
  insert_user,
  receive_response,
  query_users,
} from '../wasm-pkg/example_sync_wasm';

// ── Types ─────────────────────────────────────────────────────────

export type SyncStatus = 'pending' | 'confirmed' | 'error';

export interface User {
  id: number;
  name: string;
  age: number;
  sync: SyncStatus;
}

export type UserCommand = {
  type: 'Insert';
  id: number;
  name: string;
  age: number;
};

export interface SyncResult {
  status: 'confirmed' | 'rejected';
  reason?: string;
}

// ── Internal helpers ──────────────────────────────────────────────

let wasmReady = false;

function readUsers(pendingIds: Set<number>, errorIds: Set<number>): User[] {
  if (!wasmReady) return [];
  try {
    const raw: { id: number; name: string; age: number }[] = JSON.parse(query_users());
    return raw.map(u => ({
      ...u,
      sync: errorIds.has(u.id) ? 'error' : pendingIds.has(u.id) ? 'pending' : 'confirmed',
    }));
  } catch {
    return [];
  }
}

// ── Hook ──────────────────────────────────────────────────────────

export function useSync() {
  const [ready, setReady] = useState(false);
  const [users, setUsers] = useState<User[]>([]);
  const pendingIds = useRef(new Set<number>());
  const errorIds = useRef(new Set<number>());
  const idCounter = useRef(0);

  const refresh = useCallback(() => {
    setUsers(readUsers(pendingIds.current, errorIds.current));
  }, []);

  useEffect(() => {
    initWasm().then(() => {
      init();
      wasmReady = true;
      setReady(true);
      refresh();
    });
  }, [refresh]);

  const execute = useCallback(
    async (cmd: UserCommand): Promise<SyncResult> => {
      // One stream per command → each await resolves independently
      const streamId = create_stream();

      // Execute optimistically in WASM
      const requestBytes = insert_user(
        streamId,
        BigInt(cmd.id),
        cmd.name,
        BigInt(cmd.age),
      );
      pendingIds.current.add(cmd.id);
      refresh(); // UI shows "pending" immediately

      // Send to server, await confirmation
      try {
        const res = await fetch('/command', {
          method: 'POST',
          headers: { 'Content-Type': 'application/octet-stream' },
          body: requestBytes as unknown as BodyInit,
        });

        if (!res.ok) {
          pendingIds.current.delete(cmd.id);
          errorIds.current.add(cmd.id);
          refresh();
          return { status: 'rejected', reason: await res.text() };
        }

        const action = receive_response(new Uint8Array(await res.arrayBuffer()));
        pendingIds.current.delete(cmd.id);
        refresh();

        if (action.startsWith('confirmed')) {
          return { status: 'confirmed' };
        }
        errorIds.current.add(cmd.id);
        refresh();
        return { status: 'rejected', reason: action.replace('rejected:', '') };
      } catch (e) {
        pendingIds.current.delete(cmd.id);
        errorIds.current.add(cmd.id);
        refresh();
        return { status: 'rejected', reason: String(e) };
      }
    },
    [refresh],
  );

  const nextId = useCallback(() => ++idCounter.current, []);

  return { ready, users, execute, nextId };
}
