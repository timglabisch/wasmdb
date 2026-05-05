import { execute, executeOnStream, nextId } from '@wasmdb/client';
import type { TableSpec } from './types';

function fireCmd(cmd: unknown, streamId?: number) {
  if (streamId !== undefined) {
    executeOnStream(streamId, cmd);
  } else {
    execute(cmd);
  }
}

const STATUS_OPTIONS = [
  { value: 'online', label: 'online' },
  { value: 'busy', label: 'busy' },
  { value: 'away', label: 'away' },
];

export const USERS_SPEC: TableSpec = {
  table: 'users',
  label: 'users',
  orderBy: 'users.name',
  columns: [
    { key: 'id', header: 'id', kind: 'id' },
    {
      key: 'name', header: 'name', kind: 'text',
      onSave: (id, name) => { execute({ type: 'UpdateUserName', id, name }); },
    },
    {
      key: 'status', header: 'status', kind: 'enum', options: STATUS_OPTIONS,
      onSave: (id, status) => { execute({ type: 'UpdateUserStatus', id, status }); },
    },
  ],
  rowAction: {
    label: '×',
    tooltip: 'DeleteUser',
    fire: (id) => execute({ type: 'DeleteUser', id }),
  },
  create: {
    label: 'CreateUser',
    fields: [
      { key: 'name', kind: 'text', placeholder: 'name' },
      { key: 'status', kind: 'enum', options: STATUS_OPTIONS },
    ],
    fire: (v, streamId) => fireCmd({
      type: 'CreateUser',
      id: nextId(),
      name: String(v.name).trim(),
      status: String(v.status),
    }, streamId),
  },
};

export const ROOMS_SPEC: TableSpec = {
  table: 'rooms',
  label: 'rooms',
  orderBy: 'rooms.name',
  columns: [
    { key: 'id', header: 'id', kind: 'id' },
    {
      key: 'name', header: 'name', kind: 'text',
      onSave: (id, name) => { execute({ type: 'RenameRoom', id, name }); },
    },
    {
      key: 'owner_user_id', header: 'owner', kind: 'fk', ref: 'users',
      onSave: (id, owner_user_id) => { execute({ type: 'TransferRoom', id, owner_user_id }); },
    },
  ],
  rowActionDisabledTooltip: 'no DeleteRoom command exposed',
  create: {
    label: 'CreateRoom',
    fields: [
      { key: 'name', kind: 'text', placeholder: 'name' },
      { key: 'owner_user_id', kind: 'fk', ref: 'users' },
    ],
    fire: (v, streamId) => fireCmd({
      type: 'CreateRoom',
      id: nextId(),
      name: String(v.name).trim(),
      owner_user_id: String(v.owner_user_id),
    }, streamId),
  },
};

export const COUNTERS_SPEC: TableSpec = {
  table: 'counters',
  label: 'counters',
  orderBy: 'counters.label',
  columns: [
    { key: 'id', header: 'id', kind: 'id' },
    {
      key: 'label', header: 'label', kind: 'text',
      onSave: (id, label) => { execute({ type: 'UpdateCounterLabel', id, label }); },
    },
    {
      key: 'value', header: 'value', kind: 'number',
      onSave: (id, value) => { execute({ type: 'SetCounterValue', id, value }); },
    },
  ],
  rowActionDisabledTooltip: 'no DeleteCounter command exposed',
  rowActionExtras: (id, row) => {
    const value = Number(row.value ?? 0);
    return (
      <>
        <button
          className="counter-step"
          data-testid={`exp-counters-inc-${id}`}
          title="+1"
          onClick={() => execute({ type: 'SetCounterValue', id, value: value + 1 })}
        >+1</button>
        <button
          className="counter-step"
          data-testid={`exp-counters-dec-${id}`}
          title="-1"
          onClick={() => execute({ type: 'SetCounterValue', id, value: value - 1 })}
        >−1</button>
      </>
    );
  },
  create: {
    label: 'CreateCounter',
    fields: [
      { key: 'label', kind: 'text', placeholder: 'label' },
      { key: 'value', kind: 'number', placeholder: 'value', defaultValue: 0 },
    ],
    fire: (v, streamId) => fireCmd({
      type: 'CreateCounter',
      id: nextId(),
      label: String(v.label).trim(),
      value: Number(v.value),
    }, streamId),
  },
};

export const MESSAGES_SPEC: TableSpec = {
  table: 'messages',
  label: 'messages',
  orderBy: 'messages.created_at',
  columns: [
    { key: 'id', header: 'id', kind: 'id' },
    {
      key: 'room_id', header: 'room', kind: 'fk', ref: 'rooms',
      onSave: (id, room_id) => { execute({ type: 'MoveMessage', id, room_id }); },
    },
    {
      key: 'author_user_id', header: 'author', kind: 'fk', ref: 'users',
      onSave: (id, author_user_id) => { execute({ type: 'UpdateMessageAuthor', id, author_user_id }); },
    },
    {
      key: 'body', header: 'body', kind: 'text',
      onSave: (id, body) => { execute({ type: 'UpdateMessageBody', id, body }); },
    },
    {
      key: 'created_at', header: 'created', kind: 'text', mono: true,
      onSave: (id, created_at) => { execute({ type: 'UpdateMessageCreatedAt', id, created_at }); },
    },
  ],
  rowAction: {
    label: '×',
    tooltip: 'DeleteMessage',
    fire: (id) => execute({ type: 'DeleteMessage', id }),
  },
  create: {
    label: 'AddMessage',
    fields: [
      { key: 'room_id', kind: 'fk', ref: 'rooms' },
      { key: 'author_user_id', kind: 'fk', ref: 'users' },
      { key: 'body', kind: 'text', placeholder: 'body' },
    ],
    fire: (v, streamId) => fireCmd({
      type: 'AddMessage',
      id: nextId(),
      room_id: String(v.room_id),
      author_user_id: String(v.author_user_id),
      body: String(v.body).trim(),
      created_at: new Date().toISOString(),
    }, streamId),
  },
};

export const ALL_SPECS: TableSpec[] = [
  USERS_SPEC,
  ROOMS_SPEC,
  COUNTERS_SPEC,
  MESSAGES_SPEC,
];
