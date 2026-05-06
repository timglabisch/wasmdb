import type { Scenario } from '@wasmdb/scenarios';
import { RoomList } from '../../components/RoomList';
import { SEED } from '../../seed';
import { BtnRenameUserA } from '../buttons';
import { ROOM_ROW_SQL } from '../components/queries';

export const roomCrossTable: Scenario = {
  id: 'room-cross-table',
  category: 'joins',
  title: 'Cross-table: rename user re-fires UserBadges, RoomRow stays quiet',
  summary:
    'RoomRow reads `rooms.*` only; the embedded UserBadge subscribes independently to `users`. Updating Alice must re-fire every UserBadge:Alice instance but leave RoomRow alone — RoomRow does not subscribe to `users`.',
  expectations: [
    'Click "Rename Alice".',
    'UserBadge:Alice instances tick (badge text updates).',
    'RoomRow:R1, RoomRow:R3 stay quiet.',
  ],
  shouldRender: [`*UserBadge:${SEED.users.A}*`],
  shouldStayQuiet: ['RoomRow:*'],
  subscriptions: [
    { component: 'RoomRow:*', sql: ROOM_ROW_SQL, note: 'No JOIN to users — independent reactive scope.' },
    {
      component: 'UserBadge:* (inside RoomRow)',
      sql: `SELECT users.id, users.name, users.status
FROM users
WHERE REACTIVE(users.id = UUID '<owner-user-id>')`,
    },
  ],
  Body: () => (
    <>
      <RoomList />
      <div className="row">
        <BtnRenameUserA />
      </div>
    </>
  ),
};
