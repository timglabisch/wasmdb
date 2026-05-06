import { useEffect, useRef } from 'react';

/**
 * Render-counter hook. Increments on every render and mirrors the count
 * into `window.__renderLog` so e2e harnesses can read it via
 * `page.evaluate(() => window.__renderLog)`. The window write is best-effort
 * and never throws.
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

/**
 * Visual feedback. Attach the returned ref to an element; after every render
 * past the first, the element briefly pulses (CSS animation `render-flash`).
 * Pure decoration — does not affect render counts.
 *
 * Skipping the first render keeps the page from flashing on initial mount,
 * which would otherwise drown the signal we actually care about
 * (re-renders triggered by reactivity).
 */
export function useRenderFlash<T extends HTMLElement>() {
  const ref = useRef<T>(null);
  const isFirst = useRef(true);
  useEffect(() => {
    if (isFirst.current) {
      isFirst.current = false;
      return;
    }
    const el = ref.current;
    if (!el) return;
    el.classList.remove('rendered-flash');
    void el.offsetHeight;
    el.classList.add('rendered-flash');
    const t = window.setTimeout(() => el.classList.remove('rendered-flash'), 700);
    return () => window.clearTimeout(t);
  });
  return ref;
}

declare global {
  interface Window {
    __renderLog?: Map<string, number>;
  }
}
