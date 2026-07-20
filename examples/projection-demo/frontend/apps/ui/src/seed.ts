import { execute } from '@wasmdb/client';
import { postEntry } from 'projection-demo-generated/ProjectionDemoCommandFactories';

/**
 * A tiny opening balance so the first paint isn't empty. Fixed command
 * ids keep it idempotent within a session (PK = command_id); the wasm DB
 * is in-memory, so a page reload starts fresh and re-seeds.
 */
const SEED: Array<{ id: string; account: string; amount_cents: number }> = [
  { id: '00000000-0000-0000-0000-0000000000a1', account: 'alice', amount_cents: 5000 },
  { id: '00000000-0000-0000-0000-0000000000a2', account: 'alice', amount_cents: -1250 },
  { id: '00000000-0000-0000-0000-0000000000b1', account: 'bob', amount_cents: 10000 },
  { id: '00000000-0000-0000-0000-0000000000b2', account: 'bob', amount_cents: -2000 },
  { id: '00000000-0000-0000-0000-0000000000b3', account: 'bob', amount_cents: 750 },
];

export async function seed(): Promise<void> {
  for (const entry of SEED) {
    await execute(postEntry(entry)).confirmed;
  }
}
