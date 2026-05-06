import type { Scenario } from '@wasmdb/scenarios';

import { counterIsolation } from './reactivity/counter-isolation';
import { counterSingleRow } from './reactivity/counter-single-row';
import { roomRename } from './reactivity/room-rename';
import { userMultiInstance } from './reactivity/user-multi-instance';
import { userNoOpWrite } from './reactivity/user-no-op-write';
import { userStatusChange } from './reactivity/user-status-change';

import { userOnlineFilter } from './filters/user-online-filter';
import { userUnknownId } from './filters/user-unknown-id';

import { joinReactive } from './joins/join-reactive';
import { joinSubqueryExists } from './joins/join-subquery-exists';
import { roomCrossTable } from './joins/room-cross-table';
import { roomTransfer } from './joins/room-transfer';

import { counterExactCount } from './aggregates/counter-exact-count';
import { msgCount } from './aggregates/msg-count';

import { msgInsertMiddle } from './ordering/msg-insert-middle';
import { roomReorder } from './ordering/room-reorder';

import { msgBulkDelete } from './batching/msg-bulk-delete';
import { msgBulkInsert } from './batching/msg-bulk-insert';
import { userBatch } from './batching/user-batch';

import { hookIdSwap } from './lifecycle/hook-id-swap';
import { hookPeekQuery } from './lifecycle/hook-peek-query';
import { msgDelete } from './lifecycle/msg-delete';
import { msgListMembership } from './lifecycle/msg-list-membership';
import { msgMove } from './lifecycle/msg-move';
import { msgUnmountInflight } from './lifecycle/msg-unmount-inflight';
import { roomTransferQuiet } from './lifecycle/room-transfer-quiet';

export const SCENARIOS: Scenario[] = [
  // reactivity
  counterSingleRow,
  counterIsolation,
  roomRename,
  userNoOpWrite,
  userMultiInstance,
  userStatusChange,
  // filters
  userOnlineFilter,
  userUnknownId,
  // joins
  joinReactive,
  joinSubqueryExists,
  roomTransfer,
  roomCrossTable,
  // aggregates
  counterExactCount,
  msgCount,
  // ordering
  roomReorder,
  msgInsertMiddle,
  // batching
  userBatch,
  msgBulkInsert,
  msgBulkDelete,
  // lifecycle
  hookPeekQuery,
  hookIdSwap,
  msgListMembership,
  msgDelete,
  msgMove,
  msgUnmountInflight,
  roomTransferQuiet,
];

export const SCENARIOS_BY_ID: Record<string, Scenario> = Object.fromEntries(
  SCENARIOS.map((s) => [s.id, s]),
);
