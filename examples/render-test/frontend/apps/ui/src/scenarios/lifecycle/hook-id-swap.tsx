import type { Scenario } from '@wasmdb/scenarios';
import { IdSwapProbe } from '../../components/IdSwapProbe';
import { BtnRenameUserA, BtnRenameUserB } from '../buttons';

export const hookIdSwap: Scenario = {
  id: 'hook-id-swap',
  category: 'lifecycle',
  title: 'useQuery re-binds when its id prop changes',
  summary:
    'The id is interpolated into SQL, so the query string itself changes when the prop changes → useQuery tears down the old subscription and registers a new one. After swapping A→B: UpdateUserName(A) must not tick the inner probe (its sub points at Bob now); UpdateUserName(B) must tick the inner probe.',
  expectations: [
    'Click "Swap to Bob" → inner probe re-renders, now displaying Bob.',
    'Click "Rename Alice" → inner probe stays quiet (A subscription gone).',
    'Click "Rename Bob" → inner probe ticks (B subscription registered).',
  ],
  subscriptions: [
    {
      component: 'IdSwapProbe:inner',
      sql: `SELECT users.name
FROM users
WHERE REACTIVE(users.id = UUID '<currently-tracked-id>')`,
      note: 'When the parent passes a different id, the SQL string itself changes → useQuery tears the old sub down and registers a fresh one.',
    },
  ],
  Body: () => (
    <>
      <section className="panel">
        <h2>Id-swap probe</h2>
        <IdSwapProbe />
      </section>
      <div className="row">
        <BtnRenameUserA />
        <BtnRenameUserB />
      </div>
    </>
  ),
};
