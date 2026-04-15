import { useState, useEffect, useRef } from 'react';
import type { DebugSnapshot } from './types';
import { getDebugSnapshot } from './wasmDebugApi';

export function useDebugSnapshot(pollMs: number = 500): DebugSnapshot | null {
  const [snapshot, setSnapshot] = useState<DebugSnapshot | null>(null);

  useEffect(() => {
    const tick = () => {
      try {
        setSnapshot(getDebugSnapshot());
      } catch {
        // WASM not ready yet
      }
    };
    tick();
    const id = setInterval(tick, pollMs);
    return () => clearInterval(id);
  }, [pollMs]);

  return snapshot;
}

export interface HistoryPoint {
  t: number;
  memory: number;
  subCount: number;
  pendingCount: number;
}

export function useDebugHistory(snapshot: DebugSnapshot | null, maxPoints: number = 120): HistoryPoint[] {
  const historyRef = useRef<HistoryPoint[]>([]);

  if (snapshot) {
    const entry: HistoryPoint = {
      t: snapshot.timestamp,
      memory: snapshot.wasmMemoryBytes,
      subCount: snapshot.subscriptions.count,
      pendingCount: snapshot.syncStatus.total_pending,
    };
    const h = historyRef.current;
    h.push(entry);
    if (h.length > maxPoints) h.shift();
  }

  return historyRef.current;
}
