import type { Scenario } from '@wasmdb/scenarios';
import { SEED } from '../../seed';
import { BtnMoveMessage1ToR2 } from '../buttons';
import { ThreeLists } from '../components/ThreeLists';
import { MSG_LIST_SQL } from '../components/queries';

export const msgMove: Scenario = {
  id: 'msg-move',
  category: 'lifecycle',
  title: 'MoveMessage(M1, R1→R2): both lists tick, R3 stays quiet',
  summary:
    'Cross-list membership move. M1 transitions from R1 to R2 by updating its `room_id`. Both lists must observe their own membership change. R3 has nothing to do with this; it stays quiet. The MessageItem:M1 itself re-renders (its `room_id` row changed).',
  expectations: [
    'Click "Move M1: R1 → R2" → MessageList:R1 ticks (loses M1).',
    'MessageList:R2 ticks (gains M1).',
    'MessageList:R3 stays quiet.',
  ],
  shouldRender: [
    `MessageList:${SEED.rooms.R1}`,
    `MessageList:${SEED.rooms.R2}`,
  ],
  shouldStayQuiet: [`MessageList:${SEED.rooms.R3}`],
  subscriptions: [
    { component: 'MessageList:*', sql: MSG_LIST_SQL, note: 'Each room\'s list independently observes membership changes via REACTIVE(room_id = …).' },
  ],
  Body: () => (
    <>
      <ThreeLists />
      <div className="row">
        <BtnMoveMessage1ToR2 />
      </div>
    </>
  ),
};
