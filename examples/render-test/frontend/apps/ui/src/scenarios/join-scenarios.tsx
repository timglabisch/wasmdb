import { RoomWithOwnerName } from '../components/RoomWithOwnerName';
import { RoomsWithMessages } from '../components/RoomsWithMessages';
import { SEED } from '../seed';
import {
  BtnDeleteMessage3,
  BtnRenameRoom2,
  BtnRenameUserA,
} from './buttons';
import type { Scenario } from './types';

export const joinScenarios: Scenario[] = [
  {
    id: 'join-reactive',
    category: 'joins',
    title: 'JOIN reactivity: rooms (per-row) ⨝ users (table-wide)',
    summary:
      'Reactive JOIN — each <RoomWithOwnerName> reads from BOTH `rooms` (per-row REACTIVE on `rooms.id`) and `users` (table-wide REACTIVE on `users.id` — the join side cannot be predicate-narrowed at query binding time without knowing the FK in advance). Two complementary tests: rename a room → only that row\'s join ticks; rename a user → ALL three joins re-fire (documented engine behavior, this assertion can tighten if correlated REACTIVE filters are added).',
    expectations: [
      'Click "Rename Engineering (R2)" → join:R2 ticks; join:R1 and join:R3 stay quiet.',
      'Click "Rename Alice" → all three joins re-fire (users side is table-wide reactive).',
    ],
    Body: () => (
      <>
        <section className="panel">
          <h2>Room joins</h2>
          <RoomWithOwnerName roomId={SEED.rooms.R1} />
          <RoomWithOwnerName roomId={SEED.rooms.R2} />
          <RoomWithOwnerName roomId={SEED.rooms.R3} />
        </section>
        <div className="row">
          <BtnRenameRoom2 />
          <BtnRenameUserA />
        </div>
      </>
    ),
  },

  {
    id: 'join-subquery-exists',
    category: 'joins',
    title: 'EXISTS-style: rooms with at least one message',
    summary:
      'Subquery-style reactivity. <RoomsWithMessages> lists every room that has ≥1 message via JOIN. Both `rooms` and `messages` must be reactive sources so a messages-membership change (M3 deleted, R2 loses its only message) propagates into the rooms-driven list.',
    expectations: [
      'Initial: list shows R1 + R2 (R3 has no messages).',
      'Click "Delete only R2 message (M3)" → RoomsWithMessages re-renders; R2 drops out.',
    ],
    Body: () => (
      <>
        <RoomsWithMessages />
        <div className="row">
          <BtnDeleteMessage3 />
        </div>
      </>
    ),
  },
];
