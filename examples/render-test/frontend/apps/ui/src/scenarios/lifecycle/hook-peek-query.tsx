import type { Scenario } from '@wasmdb/scenarios';
import { PeekProbe } from '../../components/PeekProbe';
import { BtnRenameUserA } from '../buttons';

export const hookPeekQuery: Scenario = {
  id: 'hook-peek-query',
  category: 'lifecycle',
  title: 'peekQuery does not subscribe (one-shot read)',
  summary:
    '`peekQuery` is a one-shot read — it must NOT register a reactive listener. PeekProbe reads Alice\'s name once on mount; subsequent UpdateUserName(A) commands must not tick PeekProbe. The "Force re-render" button bumps local state to prove the read still picks up new values when the component is rendered for unrelated reasons.',
  expectations: [
    'Click "Rename Alice" → PeekProbe r:0 (no subscription registered).',
    'Click "Force re-render" → PeekProbe ticks once; the displayed name now reflects the latest value.',
  ],
  shouldStayQuiet: ['PeekProbe'],
  subscriptions: [
    {
      component: 'PeekProbe',
      sql: `SELECT users.name FROM users WHERE users.id = :id   /* peekQuery */`,
      note: '`peekQuery` is non-reactive: it pulls a snapshot, no subscription is registered. Re-evaluation only happens when the component renders for some other reason.',
    },
  ],
  Body: () => (
    <>
      <section className="panel">
        <h2>Peek probe</h2>
        <PeekProbe />
      </section>
      <div className="row">
        <BtnRenameUserA />
      </div>
    </>
  ),
};
