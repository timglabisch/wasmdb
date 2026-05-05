import { CounterPanel } from '../components/CounterPanel';
import { MessageList } from '../components/MessageList';
import { RoomList } from '../components/RoomList';
import { SEED } from '../seed';
import { BtnIncrementC1, BtnIncrementC2 } from './buttons';
import type { Scenario } from './types';

const COUNTER_IDS = [
  SEED.counters.C1,
  SEED.counters.C2,
  SEED.counters.C3,
  SEED.counters.C4,
];

export const counterScenarios: Scenario[] = [
  {
    id: 'counter-single-row',
    category: 'counters',
    title: 'Single-row update targets only its row',
    summary:
      'A SetCounterValue(C1) command must re-render only Counter:C1. The other three counters share the same React tree but read different rows — their reactive predicates do not match, so they stay quiet.',
    expectations: [
      'Click "+1 Counter 1" → Counter:C1 ticks (typically r:+2: optimistic + confirmed echo).',
      'Counter:C2/C3/C4 stay at r:0.',
      'The counter\'s displayed value increments by 1.',
    ],
    Body: () => (
      <>
        <CounterPanel ids={COUNTER_IDS} />
        <div className="row">
          <BtnIncrementC1 />
        </div>
      </>
    ),
  },

  {
    id: 'counter-exact-count',
    category: 'counters',
    title: 'Exact render count: optimistic + confirmed = 2',
    summary:
      'Regression-fence on render count. The echo-server flow performs one optimistic apply (1 render) followed by a confirmed echo apply of the same delta (1 render) → exactly 2 renders per command. If this drops to 1 or grows to 3+, the reactivity pump or echo-apply changed.',
    expectations: [
      'Click "+1 Counter 1" → Counter:C1 increments by exactly r:+2.',
    ],
    Body: () => (
      <>
        <CounterPanel ids={[SEED.counters.C1]} />
        <div className="row">
          <BtnIncrementC1 />
        </div>
      </>
    ),
  },

  {
    id: 'counter-isolation',
    category: 'counters',
    title: 'Cross-table isolation: counter writes leave everything else quiet',
    summary:
      'Negative-space test. A write to `counters` must NOT touch any unrelated component. UserBadge, RoomRow, MessageList, MessageItem all live in different reactive scopes — none of them subscribe to `counters` so none may re-render.',
    expectations: [
      'Click "+1 Counter 1" → Counter:C1 ticks.',
      'No RoomRow, no UserBadge, no MessageList, no MessageItem ticks.',
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
  },
];
