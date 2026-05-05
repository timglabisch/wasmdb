import { RoomList } from '../components/RoomList';
import { MessageList } from '../components/MessageList';
import { SEED } from '../seed';
import {
  BtnRenameRoom1ToAaa,
  BtnRenameRoom2,
  BtnRenameUserA,
  BtnTransferRoom1ToB,
} from './buttons';
import type { Scenario } from './types';

export const roomScenarios: Scenario[] = [
  {
    id: 'room-rename',
    category: 'rooms',
    title: 'Rename room: only that row\'s component ticks; no UserBadge ticks',
    summary:
      'Per-row column update without an FK change. Only the renamed RoomRow re-renders; its sibling rows stay quiet, and *no* UserBadge re-renders because the owner FK didn\'t change. Separates "row touched" from "FK changed".',
    expectations: [
      'Click "Rename Engineering (R2)" → RoomRow:R2 ticks.',
      'RoomRow:R1, RoomRow:R3 stay quiet.',
      'No UserBadge ticks anywhere.',
    ],
    Body: () => (
      <>
        <RoomList />
        <div className="row">
          <BtnRenameRoom2 />
        </div>
      </>
    ),
  },

  {
    id: 'room-transfer',
    category: 'rooms',
    title: 'Transfer room: ownership FK changes, only that row ticks',
    summary:
      'TransferRoom updates `owner_user_id`. RoomRow:R1\'s row data changed → it ticks. Sibling RoomRows stay quiet.',
    expectations: [
      'Click "Transfer Lobby (R1) → Bob" → RoomRow:R1 ticks; the owner badge inside it now shows Bob.',
      'RoomRow:R2, RoomRow:R3 stay quiet.',
    ],
    Body: () => (
      <>
        <RoomList />
        <div className="row">
          <BtnTransferRoom1ToB />
        </div>
      </>
    ),
  },

  {
    id: 'room-transfer-quiet',
    category: 'rooms',
    title: 'Transfer room: subscription teardown — old owner badge stays quiet elsewhere',
    summary:
      'After TransferRoom(R1, B), the UserBadge:Alice@room:R1 instance unmounts and is replaced by a fresh UserBadge:Bob@room:R1. The old A-badge subscription is torn down. Alice\'s *other* instances (R3 owner, M1 author) must stay quiet — her row didn\'t change.',
    expectations: [
      'Click "Transfer Lobby (R1) → Bob".',
      'A new UserBadge:Bob@room:R1 mounts and renders.',
      'UserBadge:Alice@room:R3 stays quiet.',
      'UserBadge:Alice@msg:M1 stays quiet.',
    ],
    Body: () => (
      <>
        <RoomList />
        <section className="panel">
          <h2>R1 messages (M1 author = Alice)</h2>
          <MessageList roomId={SEED.rooms.R1} />
        </section>
        <div className="row">
          <BtnTransferRoom1ToB />
        </div>
      </>
    ),
  },

  {
    id: 'room-reorder',
    category: 'rooms',
    title: 'Reorder via ORDER BY: list ticks, only the renamed row ticks',
    summary:
      'Renaming Lobby ("Lobby" → "Aaa Lobby") changes its position in `<RoomList>`\'s `ORDER BY rooms.name`. The list itself re-renders (membership in ordering changed); only RoomRow:R1 — the row whose data changed — ticks. R2/R3 rows stay quiet.',
    expectations: [
      'Click "Rename R1 → Aaa Lobby" → list reorders, "Aaa Lobby" sits first.',
      'RoomList ticks.',
      'RoomRow:R1 ticks; R2, R3 stay quiet.',
    ],
    Body: () => (
      <>
        <RoomList />
        <div className="row">
          <BtnRenameRoom1ToAaa />
        </div>
      </>
    ),
  },

  {
    id: 'room-cross-table',
    category: 'rooms',
    title: 'Cross-table: rename user re-fires UserBadges, RoomRow stays quiet',
    summary:
      'RoomRow reads `rooms.*` only; the embedded UserBadge subscribes independently to `users`. Updating Alice must re-fire every UserBadge:Alice instance but leave RoomRow alone — RoomRow does not subscribe to `users`.',
    expectations: [
      'Click "Rename Alice".',
      'UserBadge:Alice instances tick (badge text updates).',
      'RoomRow:R1, RoomRow:R3 stay quiet.',
    ],
    Body: () => (
      <>
        <RoomList />
        <div className="row">
          <BtnRenameUserA />
        </div>
      </>
    ),
  },
];
