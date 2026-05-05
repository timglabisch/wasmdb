import { useRef } from 'react';

/**
 * Render-counter fixture. Increments on every render and mirrors the count
 * into `window.__renderLog` so Playwright can read it via
 * `page.evaluate(() => window.__renderLog)`.
 *
 * Names are caller-controlled and should encode identity:
 *   useRenderCount(`RoomRow:${id}`)
 *   useRenderCount(`UserBadge:${id}`)
 *
 * Component MUST NOT be wrapped in `<StrictMode>` — strict-mode double
 * renders would invalidate the counts.
 */
export function useRenderCount(name: string): number {
  const ref = useRef(0);
  ref.current += 1;
  if (typeof window !== 'undefined') {
    const w = window as unknown as { __renderLog?: Map<string, number> };
    const log = w.__renderLog ?? (w.__renderLog = new Map<string, number>());
    log.set(name, (log.get(name) ?? 0) + 1);
  }
  return ref.current;
}

/** Reset all counts. Call from Playwright before each scenario. */
export function resetRenderLog(): void {
  if (typeof window !== 'undefined') {
    const w = window as unknown as { __renderLog?: Map<string, number> };
    w.__renderLog = new Map();
  }
}

declare global {
  interface Window {
    __renderLog?: Map<string, number>;
    __resetRenderLog?: () => void;
  }
}
