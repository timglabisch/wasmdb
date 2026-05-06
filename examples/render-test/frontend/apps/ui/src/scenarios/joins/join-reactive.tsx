import type { Scenario } from '@wasmdb/scenarios';
import { RoomWithOwnerName } from '../../components/RoomWithOwnerName';
import { SEED } from '../../seed';
import { BtnRenameRoom2, BtnRenameUserA } from '../buttons';

export const joinReactive: Scenario = {
  id: 'join-reactive',
  category: 'joins',
  title: 'JOIN reactivity: rooms (per-row) ⨝ users (table-wide)',
  summary:
    'Reactive JOIN. Each <RoomWithOwnerName> reads from BOTH `rooms` (per-row REACTIVE on rooms.id) and `users` (table-wide REACTIVE on users.id — the join side cannot be predicate-narrowed at query binding time without knowing the FK in advance). Two complementary buttons: rename a room → only that row\'s join ticks; rename a user → ALL three joins re-fire (documented engine behavior, this assertion can tighten if correlated REACTIVE filters are added).',
  expectations: [
    'Click "Rename Engineering (R2)" → join:R2 ticks; join:R1 and join:R3 stay quiet.',
    'Click "Rename Alice" → all three joins re-fire (users side is table-wide reactive).',
  ],
  shouldRender: [`RoomWithOwnerName:${SEED.rooms.R2}`],
  shouldStayQuiet: [
    `RoomWithOwnerName:${SEED.rooms.R1}`,
    `RoomWithOwnerName:${SEED.rooms.R3}`,
  ],
  subscriptions: [
    {
      component: 'RoomWithOwnerName:*',
      sql: `SELECT REACTIVE(users.id), rooms.name, users.name
FROM rooms
JOIN users ON users.id = rooms.owner_user_id
WHERE REACTIVE(rooms.id = UUID '<room-id>')`,
      note: 'Predicate `rooms.id = …` is per-row → narrow. The join side `REACTIVE(users.id)` (table-wide) cannot be narrowed without knowing the FK at binding time.',
    },
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
};
