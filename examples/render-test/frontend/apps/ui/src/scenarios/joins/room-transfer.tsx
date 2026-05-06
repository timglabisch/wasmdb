import type { Scenario } from '@wasmdb/scenarios';
import { RoomList } from '../../components/RoomList';
import { SEED } from '../../seed';
import { BtnTransferRoom1ToB } from '../buttons';
import { ROOM_ROW_SQL } from '../components/queries';

export const roomTransfer: Scenario = {
  id: 'room-transfer',
  category: 'joins',
  title: 'Transfer room: ownership FK changes, only that row ticks',
  summary:
    'TransferRoom updates `owner_user_id`. RoomRow:R1\'s row data changed → it ticks. Sibling RoomRows stay quiet.',
  expectations: [
    'Click "Transfer Lobby (R1) → Bob" → RoomRow:R1 ticks; the owner badge inside it now shows Bob.',
    'RoomRow:R2, RoomRow:R3 stay quiet.',
  ],
  shouldRender: [`RoomRow:${SEED.rooms.R1}`],
  shouldStayQuiet: [`RoomRow:${SEED.rooms.R2}`, `RoomRow:${SEED.rooms.R3}`],
  subscriptions: [
    { component: 'RoomRow:*', sql: ROOM_ROW_SQL },
  ],
  Body: () => (
    <>
      <RoomList />
      <div className="row">
        <BtnTransferRoom1ToB />
      </div>
    </>
  ),
};
