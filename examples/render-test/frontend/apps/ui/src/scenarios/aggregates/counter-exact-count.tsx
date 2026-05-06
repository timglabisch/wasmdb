import type { Scenario } from '@wasmdb/scenarios';
import { CounterPanel } from '../../components/CounterPanel';
import { SEED } from '../../seed';
import { BtnIncrementC1 } from '../buttons';

export const counterExactCount: Scenario = {
  id: 'counter-exact-count',
  category: 'aggregates',
  title: 'Exact render count: optimistic + confirmed = 2',
  summary:
    'Regression-fence on render count. The echo-server flow performs one optimistic apply (1 render) followed by a confirmed echo apply of the same delta (1 render) → exactly 2 renders per command. If this drops to 1 or grows to 3+, the reactivity pump or echo-apply changed.',
  expectations: [
    'Click "+1 Counter 1" → Counter:C1 increments by exactly r:+2.',
  ],
  shouldRender: [`Counter:${SEED.counters.C1}`],
  subscriptions: [
    {
      component: 'Counter:C1',
      sql: `SELECT counters.id, counters.label, counters.value
FROM counters
WHERE REACTIVE(counters.id = UUID '<C1>')`,
      note: 'Echo-server semantics: each command produces an optimistic apply + confirmed apply. The delta is identical, so React renders twice with the same value.',
    },
  ],
  Body: () => (
    <>
      <CounterPanel ids={[SEED.counters.C1]} />
      <div className="row">
        <BtnIncrementC1 />
      </div>
    </>
  ),
};
