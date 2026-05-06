import type { Scenario } from '@wasmdb/scenarios';
import { RoomList } from '../../components/RoomList';
import { SEED } from '../../seed';
import { BtnRenameRoom1ToAaa } from '../buttons';
import { ROOM_LIST_SQL, ROOM_ROW_SQL } from '../components/queries';

export const roomReorder: Scenario = {
  id: 'room-reorder',
  category: 'ordering',
  title: 'Reorder via ORDER BY: list ticks, only the renamed row ticks',
  summary:
    'Renaming Lobby ("Lobby" → "Aaa Lobby") changes its position in <RoomList>\'s `ORDER BY rooms.name`. The list itself re-renders (membership in ordering changed); only RoomRow:R1 — the row whose data changed — ticks. R2/R3 rows stay quiet.',
  expectations: [
    'Click "Rename R1 → Aaa Lobby" → list reorders, "Aaa Lobby" sits first.',
    'RoomList ticks.',
    'RoomRow:R1 ticks; R2, R3 stay quiet.',
  ],
  shouldRender: [`RoomRow:${SEED.rooms.R1}`, 'RoomList'],
  shouldStayQuiet: [`RoomRow:${SEED.rooms.R2}`, `RoomRow:${SEED.rooms.R3}`],
  subscriptions: [
    { component: 'RoomList', sql: ROOM_LIST_SQL },
    { component: 'RoomRow:*', sql: ROOM_ROW_SQL },
  ],
  Body: () => (
    <>
      <RoomList />
      <div className="row">
        <BtnRenameRoom1ToAaa />
      </div>
    </>
  ),
};
