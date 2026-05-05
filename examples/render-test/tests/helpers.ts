import type { Page } from '@playwright/test';

export type RenderLog = Record<string, number>;

/** Read render counts from the page's `window.__renderLog`. */
export async function readRenderLog(page: Page): Promise<RenderLog> {
  return page.evaluate(() => {
    const log = (window as any).__renderLog as Map<string, number> | undefined;
    return log ? Object.fromEntries(log) : {};
  });
}

/** Reset render counts. App keeps running; only the log is wiped. */
export async function resetRenderLog(page: Page): Promise<void> {
  await page.evaluate(() => {
    const reset = (window as any).__resetRenderLog as (() => void) | undefined;
    reset?.();
  });
}

/** Wait until the seed has finished and the app is interactive. */
export async function waitForAppReady(page: Page): Promise<void> {
  await page.waitForSelector('[data-testid=app-ready]', { timeout: 30_000 });
}

/**
 * Diff two render-logs. Returns a map of `name → delta` for every key
 * that changed. Keys with delta 0 are dropped. Useful for assertions
 * like `expect(diff).toEqual({ 'Counter:C1': 1 })`.
 */
export function diffLogs(before: RenderLog, after: RenderLog): RenderLog {
  const out: RenderLog = {};
  const keys = new Set([...Object.keys(before), ...Object.keys(after)]);
  for (const k of keys) {
    const d = (after[k] ?? 0) - (before[k] ?? 0);
    if (d !== 0) out[k] = d;
  }
  return out;
}

/**
 * Wait one event-loop turn beyond reactivity drain. The drain pump runs
 * via `queueMicrotask`; React commits on the next tick. Two short waits
 * is enough in practice — bumping if scenarios prove flaky.
 */
export async function settleReactivity(page: Page): Promise<void> {
  await page.waitForTimeout(50);
  await page.evaluate(() => new Promise((r) => requestAnimationFrame(() => r(undefined))));
  await page.waitForTimeout(50);
}
