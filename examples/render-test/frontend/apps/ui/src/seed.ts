import { execute } from '@wasmdb/client';
import type { RenderTestCommand } from 'render-test-generated/RenderTestCommand';

/**
 * Deterministic seed. UUIDs are hand-rolled hex so Playwright can reference
 * them by name — no `nextId()` randomness in fixture data.
 *
 * Layout:
 *   Users:    A, B, C
 *   Rooms:    R1 (owner=A), R2 (owner=B), R3 (owner=A)
 *   Counters: C1 (value=0), C2 (value=0), C3 (value=0), C4 (value=0)
 *   Messages: M1 in R1 by A, M2 in R1 by B, M3 in R2 by C
 */

export const SEED = {
  users: {
    A: '00000000-0000-0000-0000-0000000000aa',
    B: '00000000-0000-0000-0000-0000000000bb',
    C: '00000000-0000-0000-0000-0000000000cc',
  },
  rooms: {
    R1: '00000000-0000-0000-0000-000000000011',
    R2: '00000000-0000-0000-0000-000000000022',
    R3: '00000000-0000-0000-0000-000000000033',
  },
  counters: {
    C1: '00000000-0000-0000-0000-0000000000c1',
    C2: '00000000-0000-0000-0000-0000000000c2',
    C3: '00000000-0000-0000-0000-0000000000c3',
    C4: '00000000-0000-0000-0000-0000000000c4',
  },
  messages: {
    M1: '00000000-0000-0000-0000-000000000fa1',
    M2: '00000000-0000-0000-0000-000000000fa2',
    M3: '00000000-0000-0000-0000-000000000fa3',
  },
};

export async function seed(): Promise<void> {
  const cmds: RenderTestCommand[] = [
    { type: 'CreateUser', id: SEED.users.A, name: 'Alice', status: 'online' },
    { type: 'CreateUser', id: SEED.users.B, name: 'Bob', status: 'online' },
    { type: 'CreateUser', id: SEED.users.C, name: 'Carol', status: 'away' },

    { type: 'CreateRoom', id: SEED.rooms.R1, name: 'Lobby', owner_user_id: SEED.users.A },
    { type: 'CreateRoom', id: SEED.rooms.R2, name: 'Engineering', owner_user_id: SEED.users.B },
    { type: 'CreateRoom', id: SEED.rooms.R3, name: 'Lounge', owner_user_id: SEED.users.A },

    { type: 'CreateCounter', id: SEED.counters.C1, label: 'Counter 1', value: 0 },
    { type: 'CreateCounter', id: SEED.counters.C2, label: 'Counter 2', value: 0 },
    { type: 'CreateCounter', id: SEED.counters.C3, label: 'Counter 3', value: 0 },
    { type: 'CreateCounter', id: SEED.counters.C4, label: 'Counter 4', value: 0 },

    {
      type: 'AddMessage',
      id: SEED.messages.M1,
      room_id: SEED.rooms.R1,
      author_user_id: SEED.users.A,
      body: 'Hello',
      created_at: '2026-01-01T00:00:00Z',
    },
    {
      type: 'AddMessage',
      id: SEED.messages.M2,
      room_id: SEED.rooms.R1,
      author_user_id: SEED.users.B,
      body: 'Hi',
      created_at: '2026-01-01T00:01:00Z',
    },
    {
      type: 'AddMessage',
      id: SEED.messages.M3,
      room_id: SEED.rooms.R2,
      author_user_id: SEED.users.C,
      body: 'Sup',
      created_at: '2026-01-01T00:02:00Z',
    },
  ];

  for (const cmd of cmds) {
    await execute(cmd).confirmed;
  }
}
