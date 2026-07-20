import {
  activateProjection,
  bootstrap,
  deactivateProjection,
  execute,
  nextId,
  repairChain,
} from '@wasmdb/client';
import { postEntry } from 'projection-demo-generated/ProjectionDemoCommandFactories';

/**
 * Post one ledger entry. `amountCents` is signed: positive deposits,
 * negative withdraws. Fires the append command optimistically — the log
 * row shows up instantly as pending; the server confirms it (committed).
 *
 * Once confirmed, run gap-repair (design §11.4): if the server linked this
 * row onto a chain head we never fetched — e.g. posting to `carol`, whose
 * history another writer seeded server-side — the committed row points at
 * an unknown `server_parent_id`. `repairChain` walks that chain backward,
 * backfilling the missing committed rows until it's contiguous from ROOT,
 * and the balance re-folds to include them. In production this would run
 * automatically at the confirm chokepoint; here it's explicit so the
 * backfill is observable.
 */
export function post(account: string, amountCents: number): void {
  const { confirmed } = execute(postEntry({ id: nextId(), account, amount_cents: amountCents }));
  void confirmed.then(() => repairChain('ledger_log'));
}

/**
 * Simulate another writer advancing `carol` behind our back: `POST
 * /foreign-write` appends a burst of committed entries server-side that no
 * client holds, then `bootstrap` re-syncs the chain heads and walks the new
 * ancestors in — a live gap-repair (design §11.4) mid-session, without a
 * reload. Resolves to the number of rows the repair pulled in.
 */
export async function foreignWriteCarol(): Promise<number> {
  const res = await fetch('/foreign-write', { method: 'POST' });
  if (!res.ok) throw new Error(`foreign-write failed: ${res.status}`);
  return bootstrap('ledger_log');
}

/**
 * Activate the demand-driven `activity` instance for one account (design
 * §12): the engine materializes `account_activity` for exactly this
 * account from local data and keeps it in sync until deactivated. The
 * instance name is ONE compound identifier — `['account', <name>]`.
 */
export function activateAccountActivity(account: string): void {
  activateProjection('activity', ['account', account]);
}

/** Release the account's `activity` instance; its output row is retracted. */
export function deactivateAccountActivity(account: string): void {
  deactivateProjection('activity', ['account', account]);
}
