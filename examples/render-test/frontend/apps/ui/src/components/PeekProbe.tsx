import { memo, useState } from 'react';
import { peekQuery } from '@wasmdb/client';
import { useRenderCount } from '../test-utils/useRenderCount';
import { useRenderFlash } from '../test-utils/useRenderFlash';
import { SEED } from '../seed';

/**
 * `peekQuery` is non-reactive: it must NOT register a listener. This
 * component reads Alice's name once on mount and never re-renders on
 * subsequent `users` changes. Tests assert that updating Alice does not
 * tick `PeekProbe` (only the explicit `force` button does).
 */
export const PeekProbe = memo(function PeekProbe() {
  const renders = useRenderCount('PeekProbe');
  const flashRef = useRenderFlash<HTMLDivElement>();
  const [tick, setTick] = useState(0);
  // Re-evaluated on every render (which only happens via setTick).
  const rows = peekQuery(
    'SELECT users.name FROM users WHERE users.id = :id',
    { id: SEED.users.A },
  );
  const name = rows?.[0]?.[0] as string ?? '';
  return (
    <div ref={flashRef} data-testid="peek-probe" className="peek-probe">
      <span>Alice (peeked): {name}</span>
      <span className="renders">r:{renders}</span>
      <button
        data-testid="btn-peek-probe-force"
        onClick={() => setTick(tick + 1)}
      >
        Force re-render
      </button>
    </div>
  );
});
