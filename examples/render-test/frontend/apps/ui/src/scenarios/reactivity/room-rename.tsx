import type { Scenario } from '@wasmdb/scenarios';
import { RoomList } from '../../components/RoomList';
import { SEED } from '../../seed';
import { BtnRenameRoom2 } from '../buttons';
import { ROOM_LIST_SQL, ROOM_ROW_SQL } from '../components/queries';

export const roomRename: Scenario = {
  id: 'room-rename',
  category: 'reactivity',
  title: 'Rename room: only that row\'s component ticks; no UserBadge ticks',
  summary:
    'Per-row column update without an FK change. Only the renamed RoomRow re-renders; its sibling rows stay quiet, and *no* UserBadge re-renders because the owner FK didn\'t change. Separates "row touched" from "FK changed".',
  expectations: [
    'Click "Rename Engineering (R2)" → RoomRow:R2 ticks.',
    'RoomRow:R1, RoomRow:R3 stay quiet.',
    'No UserBadge ticks anywhere.',
  ],
  shouldRender: [`RoomRow:${SEED.rooms.R2}`],
  shouldStayQuiet: [
    `RoomRow:${SEED.rooms.R1}`,
    `RoomRow:${SEED.rooms.R3}`,
    'UserBadge:*',
  ],
  subscriptions: [
    { component: 'RoomRow:*', sql: ROOM_ROW_SQL },
    { component: 'RoomList', sql: ROOM_LIST_SQL, note: 'Table-wide REACTIVE in SELECT → fires on any rooms change.' },
  ],
  Body: () => (
    <>
      <RoomList />
      <div className="row">
        <BtnRenameRoom2 />
      </div>
    </>
  ),
};
