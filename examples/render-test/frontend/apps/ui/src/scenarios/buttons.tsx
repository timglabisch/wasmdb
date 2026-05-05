import { execute, peekQuery, nextId } from '@wasmdb/client';
import type { RenderTestCommand } from 'render-test-generated/RenderTestCommand';
import { SEED } from '../seed';
import { useAction } from './ActionTracker';

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

interface BtnProps {
  id: string;
  label: string;
  action: () => void;
  variant?: 'default' | 'danger' | 'positive';
}

/**
 * Tracked button. Wraps the user-supplied action in a snapshot/diff
 * roundtrip so the live diff panel updates after every click.
 */
function Btn({ id, label, action, variant = 'default' }: BtnProps) {
  const { track } = useAction();
  return (
    <button
      data-testid={id}
      className={`tracked-btn variant-${variant}`}
      onClick={() => track(label, action)}
    >
      {label}
    </button>
  );
}

export const BtnIncrementC1 = () => (
  <Btn
    id="btn-increment-counter-1"
    label="+1 Counter 1"
    variant="positive"
    action={() => fire({
      type: 'SetCounterValue',
      id: SEED.counters.C1,
      value: readCounter(SEED.counters.C1) + 1,
    })}
  />
);

export const BtnIncrementC2 = () => (
  <Btn
    id="btn-increment-counter-2"
    label="+1 Counter 2"
    variant="positive"
    action={() => fire({
      type: 'SetCounterValue',
      id: SEED.counters.C2,
      value: readCounter(SEED.counters.C2) + 1,
    })}
  />
);

export const BtnRenameUserA = () => (
  <Btn
    id="btn-rename-user-a"
    label="Rename Alice"
    action={() => fire({ type: 'UpdateUserName', id: SEED.users.A, name: 'Alice (renamed)' })}
  />
);

export const BtnRenameUserASame = () => (
  <Btn
    id="btn-rename-user-a-same"
    label='Rename Alice → "Alice" (no-op same value)'
    action={() => fire({ type: 'UpdateUserName', id: SEED.users.A, name: 'Alice' })}
  />
);

export const BtnRenameUserB = () => (
  <Btn
    id="btn-rename-user-b"
    label="Rename Bob"
    action={() => fire({ type: 'UpdateUserName', id: SEED.users.B, name: 'Bob (renamed)' })}
  />
);

export const BtnStatusUserABusy = () => (
  <Btn
    id="btn-status-user-a-busy"
    label="Alice → busy"
    action={() => fire({ type: 'UpdateUserStatus', id: SEED.users.A, status: 'busy' })}
  />
);

export const BtnStatusUserCOnline = () => (
  <Btn
    id="btn-status-user-c-online"
    label="Carol → online"
    variant="positive"
    action={() => fire({ type: 'UpdateUserStatus', id: SEED.users.C, status: 'online' })}
  />
);

export const BtnRenameUsersAAndB = () => (
  <Btn
    id="btn-rename-users-a-and-b"
    label="Rename Alice + Bob (one tick)"
    action={() => {
      fire({ type: 'UpdateUserName', id: SEED.users.A, name: 'Alice (batch)' });
      fire({ type: 'UpdateUserName', id: SEED.users.B, name: 'Bob (batch)' });
    }}
  />
);

export const BtnRenameUnknownUser = () => (
  <Btn
    id="btn-rename-unknown-user"
    label="Rename unknown user (id not in db)"
    action={() => fire({ type: 'UpdateUserName', id: UNKNOWN_USER, name: 'Ghost' })}
  />
);

export const BtnTransferRoom1ToB = () => (
  <Btn
    id="btn-transfer-room-1-to-b"
    label="Transfer Lobby (R1) → Bob"
    action={() => fire({ type: 'TransferRoom', id: SEED.rooms.R1, owner_user_id: SEED.users.B })}
  />
);

export const BtnRenameRoom2 = () => (
  <Btn
    id="btn-rename-room-2"
    label="Rename Engineering (R2)"
    action={() => fire({ type: 'RenameRoom', id: SEED.rooms.R2, name: 'Engineering (renamed)' })}
  />
);

export const BtnRenameRoom1ToAaa = () => (
  <Btn
    id="btn-rename-room-1-to-aaa"
    label='Rename R1 → "Aaa Lobby" (forces reorder)'
    action={() => fire({ type: 'RenameRoom', id: SEED.rooms.R1, name: 'Aaa Lobby' })}
  />
);

export const BtnAddMessageR1 = () => (
  <Btn
    id="btn-add-message-room-1"
    label="+ Message in Lobby (R1)"
    variant="positive"
    action={() => fire({
      type: 'AddMessage',
      id: nextId(),
      room_id: SEED.rooms.R1,
      author_user_id: SEED.users.A,
      body: 'New message',
      created_at: new Date().toISOString(),
    })}
  />
);

export const BtnAddMessageR1Early = () => (
  <Btn
    id="btn-add-message-r1-early"
    label="+ Early message (R1, sorts first)"
    variant="positive"
    action={() => fire({
      type: 'AddMessage',
      id: nextId(),
      room_id: SEED.rooms.R1,
      author_user_id: SEED.users.A,
      body: 'Early message',
      created_at: '2025-01-01T00:00:00Z',
    })}
  />
);

export const BtnDeleteMessage1 = () => (
  <Btn
    id="btn-delete-message-1"
    label="Delete first Lobby message (M1)"
    variant="danger"
    action={() => fire({ type: 'DeleteMessage', id: SEED.messages.M1 })}
  />
);

export const BtnDeleteMessage3 = () => (
  <Btn
    id="btn-delete-message-3"
    label="Delete only R2 message (M3)"
    variant="danger"
    action={() => fire({ type: 'DeleteMessage', id: SEED.messages.M3 })}
  />
);

export const BtnMoveMessage1ToR2 = () => (
  <Btn
    id="btn-move-message-1-to-r2"
    label="Move M1: R1 → R2"
    action={() => fire({ type: 'MoveMessage', id: SEED.messages.M1, room_id: SEED.rooms.R2 })}
  />
);

export const BtnBulkAdd20R1 = () => (
  <Btn
    id="btn-bulk-add-20-r1"
    label="+ 20 messages (R1)"
    variant="positive"
    action={() => {
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
  />
);

export const BtnBulkDeleteR1 = () => (
  <Btn
    id="btn-bulk-delete-r1"
    label="Delete bulk-added messages"
    variant="danger"
    action={() => {
      const ids = window.__bulkMessageIds ?? [];
      for (const id of ids) {
        fire({ type: 'DeleteMessage', id });
      }
      window.__bulkMessageIds = [];
    }}
  />
);
