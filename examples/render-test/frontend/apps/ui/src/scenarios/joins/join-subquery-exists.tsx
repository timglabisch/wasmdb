import type { Scenario } from '@wasmdb/scenarios';
import { RoomsWithMessages } from '../../components/RoomsWithMessages';
import { BtnDeleteMessage3 } from '../buttons';

export const joinSubqueryExists: Scenario = {
  id: 'join-subquery-exists',
  category: 'joins',
  title: 'EXISTS-style: rooms with at least one message',
  summary:
    'Subquery-style reactivity. <RoomsWithMessages> lists every room that has ≥1 message via JOIN. Both `rooms` and `messages` must be reactive sources so a messages-membership change (M3 deleted, R2 loses its only message) propagates into the rooms-driven list.',
  expectations: [
    'Initial: list shows R1 + R2 (R3 has no messages).',
    'Click "Delete only R2 message (M3)" → RoomsWithMessages re-renders; R2 drops out.',
  ],
  shouldRender: ['RoomsWithMessages'],
  subscriptions: [
    {
      component: 'RoomsWithMessages',
      sql: `SELECT REACTIVE(messages.room_id), rooms.id
FROM rooms
JOIN messages ON messages.room_id = rooms.id
ORDER BY rooms.id`,
      note: 'Membership of `messages` directly changes which rooms appear. Table-wide REACTIVE on messages.room_id covers any messages mutation.',
    },
  ],
  Body: () => (
    <>
      <RoomsWithMessages />
      <div className="row">
        <BtnDeleteMessage3 />
      </div>
    </>
  ),
};
