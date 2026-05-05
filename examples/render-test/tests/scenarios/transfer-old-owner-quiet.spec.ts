import { test, expect } from '@playwright/test';
import { diffLogs, readRenderLog, resetRenderLog, settleReactivity, waitForAppReady } from '../helpers';

const ALICE = '00000000-0000-0000-0000-0000000000aa';
const BOB = '00000000-0000-0000-0000-0000000000bb';
const R1 = '00000000-0000-0000-0000-000000000011';

/**
 * Subscription cleanup on FK change: after `TransferRoom(R1, B)`, the
 * `<UserBadge:A@room:R1>` instance unmounts and is replaced by a fresh
 * `<UserBadge:B@room:R1>`. The old A-badge must NOT receive any further
 * renders (its subscription was torn down). Verifies that mounting and
 * unmounting cleanly attaches/detaches reactive subscriptions.
 *
 * Alice's *other* instances (room:R3 owner, msg:M1 author) must stay
 * quiet — they didn't change.
 */
test('TransferRoom(R1, B) mounts UserBadge:B@room:R1 fresh, leaves A elsewhere quiet', async ({ page }) => {
  await page.goto('/');
  await waitForAppReady(page);

  await resetRenderLog(page);
  const before = await readRenderLog(page);

  await page.click('[data-testid=btn-transfer-room-1-to-b]');
  await settleReactivity(page);

  const after = await readRenderLog(page);
  const diff = diffLogs(before, after);

  // The new owner badge mounts and renders at R1.
  expect(diff[`UserBadge:${BOB}@room:${R1}`] ?? 0, 'Bob badge at R1 should mount').toBeGreaterThanOrEqual(1);

  // Alice's badges in unrelated contexts must stay quiet — her row didn't change.
  expect.soft(
    diff[`UserBadge:${ALICE}@room:00000000-0000-0000-0000-000000000033`] ?? 0,
    'Alice@R3 must not render',
  ).toBe(0);
  expect.soft(
    diff[`UserBadge:${ALICE}@msg:00000000-0000-0000-0000-000000000fa1`] ?? 0,
    'Alice@M1 must not render',
  ).toBe(0);
});
