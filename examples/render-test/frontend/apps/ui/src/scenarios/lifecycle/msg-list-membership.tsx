import type { Scenario } from '@wasmdb/scenarios';
import { SEED } from '../../seed';
import { BtnAddMessageR1 } from '../buttons';
import { ThreeLists } from '../components/ThreeLists';
import { MSG_ITEM_SQL, MSG_LIST_SQL } from '../components/queries';

export const msgListMembership: Scenario = {
  id: 'msg-list-membership',
  category: 'lifecycle',
  title: 'AddMessage(R1): only R1 list ticks; existing items stay quiet',
  summary:
    'List-add reactivity. A new row matches the per-room predicate. Only MessageList:R1 re-renders. Sibling lists for R2/R3 stay quiet. Existing M1/M2/M3 items did not change → their MessageItem components stay quiet (the new message has its own freshly-mounted item).',
  expectations: [
    'Click "+ Message in Lobby (R1)" → MessageList:R1 ticks; a new item appears.',
    'MessageList:R2, MessageList:R3 stay quiet.',
    'MessageItem:M1, MessageItem:M2, MessageItem:M3 stay quiet.',
  ],
  shouldRender: [`MessageList:${SEED.rooms.R1}`],
  shouldStayQuiet: [
    `MessageList:${SEED.rooms.R2}`,
    `MessageList:${SEED.rooms.R3}`,
    `MessageItem:${SEED.messages.M1}`,
    `MessageItem:${SEED.messages.M2}`,
    `MessageItem:${SEED.messages.M3}`,
  ],
  subscriptions: [
    { component: 'MessageList:*', sql: MSG_LIST_SQL },
    { component: 'MessageItem:*', sql: MSG_ITEM_SQL, note: 'Per-row predicate — only re-fires when its own row changes.' },
  ],
  Body: () => (
    <>
      <ThreeLists />
      <div className="row">
        <BtnAddMessageR1 />
      </div>
    </>
  ),
};
