import type { Scenario } from '@wasmdb/scenarios';
import { CounterPanel } from '../../components/CounterPanel';
import { MessageList } from '../../components/MessageList';
import { RoomList } from '../../components/RoomList';
import { SEED } from '../../seed';
import { BtnIncrementC1, BtnIncrementC2 } from '../buttons';

const COUNTER_IDS = [
  SEED.counters.C1,
  SEED.counters.C2,
  SEED.counters.C3,
  SEED.counters.C4,
];

export const counterIsolation: Scenario = {
  id: 'counter-isolation',
  category: 'reactivity',
  title: 'Cross-table isolation: counter writes leave everything else quiet',
  summary:
    'Negative-space test. A write to `counters` must NOT touch any unrelated component. UserBadge, RoomRow, MessageList, MessageItem all live in different reactive scopes — none of them subscribe to `counters` so none may re-render.',
  expectations: [
    'Click "+1 Counter 1" → Counter:C1 ticks.',
    'No RoomRow, no UserBadge, no MessageList, no MessageItem ticks.',
  ],
  shouldRender: [`Counter:${SEED.counters.C1}`],
  shouldStayQuiet: ['RoomRow:*', 'UserBadge:*', 'MessageList:*', 'MessageItem:*'],
  subscriptions: [
    {
      component: 'Counter:*',
      sql: `SELECT … FROM counters WHERE REACTIVE(counters.id = …)`,
    },
    {
      component: 'RoomRow:*',
      sql: `SELECT … FROM rooms WHERE REACTIVE(rooms.id = …)`,
      note: 'rooms ≠ counters → no shared reactive scope.',
    },
    {
      component: 'MessageList:*',
      sql: `SELECT messages.id FROM messages WHERE REACTIVE(messages.room_id = …)`,
    },
  ],
  Body: () => (
    <>
      <CounterPanel ids={COUNTER_IDS} />
      <RoomList />
      <section className="panel">
        <h2>Messages by room</h2>
        <div className="message-grid">
          <MessageList roomId={SEED.rooms.R1} />
          <MessageList roomId={SEED.rooms.R2} />
          <MessageList roomId={SEED.rooms.R3} />
        </div>
      </section>
      <div className="row">
        <BtnIncrementC1 />
        <BtnIncrementC2 />
      </div>
    </>
  ),
};
