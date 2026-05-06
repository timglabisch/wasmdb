import type { Scenario } from '@wasmdb/scenarios';
import { CounterPanel } from '../../components/CounterPanel';
import { SEED } from '../../seed';
import { BtnIncrementC1 } from '../buttons';

const COUNTER_IDS = [
  SEED.counters.C1,
  SEED.counters.C2,
  SEED.counters.C3,
  SEED.counters.C4,
];

export const counterSingleRow: Scenario = {
  id: 'counter-single-row',
  category: 'reactivity',
  title: 'Single-row update targets only its row',
  summary:
    'A SetCounterValue(C1) command must re-render only Counter:C1. The other three counters share the same React tree but read different rows — their reactive predicates do not match, so they stay quiet.',
  expectations: [
    'Click "+1 Counter 1" → Counter:C1 ticks (typically r:+2: optimistic + confirmed echo).',
    'Counter:C2/C3/C4 stay at r:0.',
    'The displayed value increments by 1.',
  ],
  shouldRender: [`Counter:${SEED.counters.C1}`],
  shouldStayQuiet: [
    `Counter:${SEED.counters.C2}`,
    `Counter:${SEED.counters.C3}`,
    `Counter:${SEED.counters.C4}`,
  ],
  subscriptions: [
    {
      component: 'Counter:C{n}',
      sql: `SELECT counters.id, counters.label, counters.value
FROM counters
WHERE REACTIVE(counters.id = UUID '<row-id>')`,
      note: 'Per-row REACTIVE predicate. Each Counter only fires when its own row changes.',
    },
  ],
  Body: () => (
    <>
      <CounterPanel ids={COUNTER_IDS} />
      <div className="row">
        <BtnIncrementC1 />
      </div>
    </>
  ),
};
