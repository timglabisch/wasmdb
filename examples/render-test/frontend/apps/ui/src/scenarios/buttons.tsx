import { execute, peekQuery, nextId } from '@wasmdb/client';
import type { RenderTestCommand } from 'render-test-generated/RenderTestCommand';
import { SEED } from '../seed';

const fire = (cmd: RenderTestCommand) => execute(cmd);

const readCounter = (id: string): number => {
  const rows = peekQuery('SELECT counters.value FROM counters WHERE counters.id = :id', { id });
  return (rows?.[0]?.[0] as number) ?? 0;
};

declare global {
  interface Window {
    __bulkMessageIds?: string[];
  }
}

const UNKNOWN_USER = '00000000-0000-0000-0000-0000000000ff';

export const BtnIncrementC1 = () => (
  <button
    data-testid="btn-increment-counter-1"
    onClick={() => fire({
      type: 'SetCounterValue',
      id: SEED.counters.C1,
      value: readCounter(SEED.counters.C1) + 1,
    })}
  >
    +1 Counter 1
  </button>
);

export const BtnIncrementC2 = () => (
  <button
    data-testid="btn-increment-counter-2"
    onClick={() => fire({
      type: 'SetCounterValue',
      id: SEED.counters.C2,
      value: readCounter(SEED.counters.C2) + 1,
    })}
  >
    +1 Counter 2
  </button>
);

export const BtnRenameUserA = () => (
  <button
    data-testid="btn-rename-user-a"
    onClick={() => fire({ type: 'UpdateUserName', id: SEED.users.A, name: 'Alice (renamed)' })}
  >
    Rename Alice
  </button>
);

export const BtnRenameUserASame = () => (
  <button
    data-testid="btn-rename-user-a-same"
    onClick={() => fire({ type: 'UpdateUserName', id: SEED.users.A, name: 'Alice' })}
  >
    Rename Alice → "Alice" (no-op same-value)
  </button>
);

export const BtnRenameUserB = () => (
  <button
    data-testid="btn-rename-user-b"
    onClick={() => fire({ type: 'UpdateUserName', id: SEED.users.B, name: 'Bob (renamed)' })}
  >
    Rename Bob
  </button>
);

export const BtnStatusUserABusy = () => (
  <button
    data-testid="btn-status-user-a-busy"
    onClick={() => fire({ type: 'UpdateUserStatus', id: SEED.users.A, status: 'busy' })}
  >
    Alice → busy
  </button>
);

export const BtnStatusUserCOnline = () => (
  <button
    data-testid="btn-status-user-c-online"
    onClick={() => fire({ type: 'UpdateUserStatus', id: SEED.users.C, status: 'online' })}
  >
    Carol → online
  </button>
);

export const BtnRenameUsersAAndB = () => (
  <button
    data-testid="btn-rename-users-a-and-b"
    onClick={() => {
      fire({ type: 'UpdateUserName', id: SEED.users.A, name: 'Alice (batch)' });
      fire({ type: 'UpdateUserName', id: SEED.users.B, name: 'Bob (batch)' });
    }}
  >
    Rename Alice + Bob (one tick)
  </button>
);

export const BtnRenameUnknownUser = () => (
  <button
    data-testid="btn-rename-unknown-user"
    onClick={() => fire({ type: 'UpdateUserName', id: UNKNOWN_USER, name: 'Ghost' })}
  >
    Rename unknown user (id not in db)
  </button>
);

export const BtnTransferRoom1ToB = () => (
  <button
    data-testid="btn-transfer-room-1-to-b"
    onClick={() => fire({ type: 'TransferRoom', id: SEED.rooms.R1, owner_user_id: SEED.users.B })}
  >
    Transfer Lobby (R1) → Bob
  </button>
);

export const BtnRenameRoom2 = () => (
  <button
    data-testid="btn-rename-room-2"
    onClick={() => fire({ type: 'RenameRoom', id: SEED.rooms.R2, name: 'Engineering (renamed)' })}
  >
    Rename Engineering (R2)
  </button>
);

export const BtnRenameRoom1ToAaa = () => (
  <button
    data-testid="btn-rename-room-1-to-aaa"
    onClick={() => fire({ type: 'RenameRoom', id: SEED.rooms.R1, name: 'Aaa Lobby' })}
  >
    Rename R1 → "Aaa Lobby" (forces reorder)
  </button>
);

export const BtnAddMessageR1 = () => (
  <button
    data-testid="btn-add-message-room-1"
    onClick={() => fire({
      type: 'AddMessage',
      id: nextId(),
      room_id: SEED.rooms.R1,
      author_user_id: SEED.users.A,
      body: 'New message',
      created_at: new Date().toISOString(),
    })}
  >
    + Message in Lobby (R1)
  </button>
);

export const BtnAddMessageR1Early = () => (
  <button
    data-testid="btn-add-message-r1-early"
    onClick={() => fire({
      type: 'AddMessage',
      id: nextId(),
      room_id: SEED.rooms.R1,
      author_user_id: SEED.users.A,
      body: 'Early message',
      created_at: '2025-01-01T00:00:00Z',
    })}
  >
    + Early message (R1, sorts first)
  </button>
);

export const BtnDeleteMessage1 = () => (
  <button
    data-testid="btn-delete-message-1"
    onClick={() => fire({ type: 'DeleteMessage', id: SEED.messages.M1 })}
  >
    Delete first Lobby message (M1)
  </button>
);

export const BtnDeleteMessage3 = () => (
  <button
    data-testid="btn-delete-message-3"
    onClick={() => fire({ type: 'DeleteMessage', id: SEED.messages.M3 })}
  >
    Delete only R2 message (M3)
  </button>
);

export const BtnMoveMessage1ToR2 = () => (
  <button
    data-testid="btn-move-message-1-to-r2"
    onClick={() => fire({ type: 'MoveMessage', id: SEED.messages.M1, room_id: SEED.rooms.R2 })}
  >
    Move M1: R1 → R2
  </button>
);

export const BtnBulkAdd20R1 = () => (
  <button
    data-testid="btn-bulk-add-20-r1"
    onClick={() => {
      const ids: string[] = [];
      for (let i = 0; i < 20; i++) {
        const id = nextId();
        ids.push(id);
        fire({
          type: 'AddMessage',
          id,
          room_id: SEED.rooms.R1,
          author_user_id: SEED.users.A,
          body: `bulk #${i}`,
          created_at: new Date(Date.now() + i).toISOString(),
        });
      }
      window.__bulkMessageIds = (window.__bulkMessageIds ?? []).concat(ids);
    }}
  >
    + 20 messages (R1)
  </button>
);

export const BtnBulkDeleteR1 = () => (
  <button
    data-testid="btn-bulk-delete-r1"
    onClick={() => {
      const ids = window.__bulkMessageIds ?? [];
      for (const id of ids) {
        fire({ type: 'DeleteMessage', id });
      }
      window.__bulkMessageIds = [];
    }}
  >
    Delete bulk-added messages
  </button>
);
