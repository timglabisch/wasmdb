import type { Scenario } from '@wasmdb/scenarios';
import { SEED } from '../../seed';
import { BtnBulkAdd20R1, BtnBulkDeleteR1 } from '../buttons';
import { ThreeLists } from '../components/ThreeLists';
import { MSG_LIST_SQL } from '../components/queries';

export const msgBulkDelete: Scenario = {
  id: 'msg-bulk-delete',
  category: 'batching',
  title: 'Bulk-insert → bulk-delete: round-trip settles cleanly',
  summary:
    'Insert 20 then delete the same 20. After settling, the list returns to its original state. Sibling rooms never observed any of the noise. Existing seed rows stayed quiet throughout. Reset render counts between phases for a clean read.',
  expectations: [
    'Click "+ 20 messages (R1)" then reset render counts.',
    'Click "Delete bulk-added messages" → MessageList:R1 ticks.',
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
  subscriptions: [{ component: 'MessageList:R1', sql: MSG_LIST_SQL }],
  Body: () => (
    <>
      <ThreeLists />
      <div className="row">
        <BtnBulkAdd20R1 />
        <BtnBulkDeleteR1 />
      </div>
    </>
  ),
};
