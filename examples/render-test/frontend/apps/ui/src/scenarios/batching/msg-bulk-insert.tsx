import type { Scenario } from '@wasmdb/scenarios';
import { SEED } from '../../seed';
import { BtnBulkAdd20R1 } from '../buttons';
import { ThreeLists } from '../components/ThreeLists';
import { MSG_LIST_SQL } from '../components/queries';

export const msgBulkInsert: Scenario = {
  id: 'msg-bulk-insert',
  category: 'batching',
  title: 'Bulk-insert 20: list renders bounded; siblings + existing rows quiet',
  summary:
    '20 inserts in one tick (synchronous burst from a single click handler). Each command produces optimistic + confirmed dirty cycles → up to ~40 list renders for the affected room. The fence guards against worse-than-linear blow-up.',
  expectations: [
    'Click "+ 20 messages (R1)" → MessageList:R1 ticks at most ~40 times.',
    'MessageList:R2, MessageList:R3 stay quiet.',
    'MessageItem:M1, MessageItem:M2 stay quiet.',
  ],
  shouldRender: [`MessageList:${SEED.rooms.R1}`],
  shouldStayQuiet: [
    `MessageList:${SEED.rooms.R2}`,
    `MessageList:${SEED.rooms.R3}`,
    `MessageItem:${SEED.messages.M1}`,
    `MessageItem:${SEED.messages.M2}`,
  ],
  subscriptions: [
    { component: 'MessageList:R1', sql: MSG_LIST_SQL, note: 'Each command = optimistic + confirmed = up to 2 renders. 20 commands → ≤40 renders.' },
  ],
  Body: () => (
    <>
      <ThreeLists />
      <div className="row">
        <BtnBulkAdd20R1 />
      </div>
    </>
  ),
};
