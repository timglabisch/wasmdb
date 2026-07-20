import { execute, nextId } from '@wasmdb/client';
import { postEntry } from 'projection-demo-generated/ProjectionDemoCommandFactories';

/**
 * Post one ledger entry. `amountCents` is signed: positive deposits,
 * negative withdraws. Fires the append command optimistically — the log
 * row shows up instantly as pending; the server confirms it (committed).
 */
export function post(account: string, amountCents: number): void {
  execute(postEntry({ id: nextId(), account, amount_cents: amountCents }));
}
