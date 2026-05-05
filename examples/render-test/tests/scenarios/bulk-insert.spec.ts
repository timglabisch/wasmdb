import { test, expect } from '@playwright/test';
import { diffLogs, readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

const R1 = '00000000-0000-0000-0000-000000000011';
const R2 = '00000000-0000-0000-0000-000000000022';
const R3 = '00000000-0000-0000-0000-000000000033';
const M1 = '00000000-0000-0000-0000-000000000fa1';
const M2 = '00000000-0000-0000-0000-000000000fa2';

/**
 * 20 inserts in one tick (synchronous burst from a single click handler).
 * Each command produces optimistic + confirmed dirty cycles. The list
 * may re-render up to ~40 times (2 per insert) — the assertion fences
 * against worse-than-linear blow-up. Sibling lists must stay quiet, and
 * existing messages must NOT re-render — their rows are untouched.
 */
test('Bulk-insert 20 messages: list renders bounded, siblings + existing rows quiet', async ({ page }) => {
  await page.goto('/');
  await waitForAppReady(page);

  await resetRenderLog(page);
  const before = await readRenderLog(page);

  await page.click('[data-testid=btn-bulk-add-20-r1]');
  await settleReactivity(page);

  const diff = diffLogs(before, await readRenderLog(page));

  const r1Renders = diff[`MessageList:${R1}`] ?? 0;
  expect(r1Renders, 'R1 list must render at least once').toBeGreaterThanOrEqual(1);
  expect(r1Renders, 'R1 list must not exceed 2× per insert (40)').toBeLessThanOrEqual(40);

  expect.soft(diff[`MessageList:${R2}`] ?? 0, 'R2 list must not render').toBe(0);
  expect.soft(diff[`MessageList:${R3}`] ?? 0, 'R3 list must not render').toBe(0);

  expect.soft(diff[`MessageItem:${M1}`] ?? 0, 'M1 must not render').toBe(0);
  expect.soft(diff[`MessageItem:${M2}`] ?? 0, 'M2 must not render').toBe(0);
});
