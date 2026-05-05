import { IdSwapProbe } from '../components/IdSwapProbe';
import { PeekProbe } from '../components/PeekProbe';
import { BtnRenameUserA, BtnRenameUserB } from './buttons';
import type { Scenario } from './types';

export const hookScenarios: Scenario[] = [
  {
    id: 'hook-peek-query',
    category: 'hooks',
    title: 'peekQuery does not subscribe (one-shot read)',
    summary:
      '`peekQuery` is a one-shot read — it must NOT register a reactive listener. PeekProbe reads Alice\'s name once on mount; subsequent UpdateUserName(A) commands must not tick PeekProbe. The "Force re-render" button bumps local state to prove the read still picks up new values when the component is rendered for unrelated reasons.',
    expectations: [
      'Click "Rename Alice" → PeekProbe r:0 (no subscription registered).',
      'Click "Force re-render" → PeekProbe ticks once; the displayed name now reflects the latest value.',
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
  },

  {
    id: 'hook-id-swap',
    category: 'hooks',
    title: 'useQuery re-binds when its id prop changes',
    summary:
      'The id is interpolated into SQL, so the query string itself changes when the prop changes → useQuery tears down the old subscription and registers a new one. After swapping A→B: UpdateUserName(A) must not tick the inner probe (its sub points at Bob now); UpdateUserName(B) must tick the inner probe.',
    expectations: [
      'Click "Swap to Bob" → inner probe re-renders, now displaying Bob.',
      'Click "Rename Alice" → inner probe stays quiet (A subscription gone).',
      'Click "Rename Bob" → inner probe ticks (B subscription registered).',
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
  },
];
