import type { Scenario } from '@wasmdb/scenarios';
import { SEED } from '../../seed';
import { BtnDeleteMessage1 } from '../buttons';
import { ThreeLists } from '../components/ThreeLists';
import { MSG_ITEM_SQL, MSG_LIST_SQL } from '../components/queries';

export const msgDelete: Scenario = {
  id: 'msg-delete',
  category: 'lifecycle',
  title: 'DeleteMessage(M1): R1 list ticks; surviving items quiet',
  summary:
    'List-shrink reactivity. A row leaves the per-room set. Only MessageList:R1 re-renders. Sibling lists stay quiet. Surviving M2/M3 items did not change → their MessageItem components stay quiet.',
  expectations: [
    'Click "Delete first Lobby message (M1)" → MessageList:R1 ticks; M1 disappears.',
    'MessageList:R2, MessageList:R3 stay quiet.',
    'MessageItem:M2, MessageItem:M3 stay quiet.',
  ],
  shouldRender: [`MessageList:${SEED.rooms.R1}`],
  shouldStayQuiet: [
    `MessageList:${SEED.rooms.R2}`,
    `MessageList:${SEED.rooms.R3}`,
    `MessageItem:${SEED.messages.M2}`,
    `MessageItem:${SEED.messages.M3}`,
  ],
  subscriptions: [
    { component: 'MessageList:*', sql: MSG_LIST_SQL },
    { component: 'MessageItem:*', sql: MSG_ITEM_SQL },
  ],
  Body: () => (
    <>
      <ThreeLists />
      <div className="row">
        <BtnDeleteMessage1 />
      </div>
    </>
  ),
};
