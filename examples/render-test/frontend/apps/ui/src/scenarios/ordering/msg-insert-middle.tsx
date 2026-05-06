import type { Scenario } from '@wasmdb/scenarios';
import { MessageList } from '../../components/MessageList';
import { SEED } from '../../seed';
import { BtnAddMessageR1Early } from '../buttons';
import { MSG_ITEM_SQL, MSG_LIST_SQL } from '../components/queries';

export const msgInsertMiddle: Scenario = {
  id: 'msg-insert-middle',
  category: 'ordering',
  title: 'Insert with earlier created_at: existing items stay quiet',
  summary:
    'Insert with a `created_at` *before* existing rows. The new MessageItem mounts at the head of the list. The existing M1/M2 rows did not change → their MessageItems must NOT re-render. The list itself re-renders (membership change).',
  expectations: [
    'Click "+ Early message (R1)" → MessageList:R1 ticks; a new item appears at the top.',
    'MessageItem:M1, MessageItem:M2 stay quiet.',
  ],
  shouldRender: [`MessageList:${SEED.rooms.R1}`],
  shouldStayQuiet: [
    `MessageItem:${SEED.messages.M1}`,
    `MessageItem:${SEED.messages.M2}`,
  ],
  subscriptions: [
    { component: 'MessageList:R1', sql: MSG_LIST_SQL, note: 'ORDER BY created_at means new rows can land anywhere. Existing rows are untouched, so their per-row subscriptions never fire.' },
    { component: 'MessageItem:*', sql: MSG_ITEM_SQL },
  ],
  Body: () => (
    <>
      <section className="panel">
        <h2>R1 messages</h2>
        <MessageList roomId={SEED.rooms.R1} />
      </section>
      <div className="row">
        <BtnAddMessageR1Early />
      </div>
    </>
  ),
};
