import { memo, useState } from 'react';
import { useQuery } from '@wasmdb/client';
import { useRenderCount, useRenderFlash } from '@wasmdb/scenarios';
import { SEED } from '../seed';

interface Props {
  id: string;
}

const Inner = memo(function Inner({ id }: Props) {
  const renders = useRenderCount(`IdSwapProbe:inner`);
  const flashRef = useRenderFlash<HTMLSpanElement>();
  const rows = useQuery<{ name: string }>(
    `SELECT users.name FROM users WHERE REACTIVE(users.id = UUID '${id}')`,
    ([name]) => ({ name: name as string }),
  );
  return (
    <span ref={flashRef} data-testid="id-swap-probe-inner">
      <span>{rows[0]?.name ?? '?'}</span>
      <span className="renders">r:{renders}</span>
    </span>
  );
});

/**
 * Tests `useQuery` re-binding when its `id` prop changes. The inner
 * memo'd component reads `users` for whichever id the parent passes.
 * Toggling A↔B must:
 *   • tear down the A subscription,
 *   • register a B subscription,
 *   • leave the inner component quiet for subsequent A-updates,
 *   • re-render the inner component on B-updates.
 */
export const IdSwapProbe = memo(function IdSwapProbe() {
  const renders = useRenderCount('IdSwapProbe');
  const [id, setId] = useState(SEED.users.A);
  return (
    <div data-testid="id-swap-probe" className="id-swap-probe">
      <span>Tracking: {id === SEED.users.A ? 'Alice' : 'Bob'}</span>
      <Inner id={id} />
      <span className="renders">r:{renders}</span>
      <button
        data-testid="btn-id-swap-to-b"
        onClick={() => setId(SEED.users.B)}
      >
        Swap to Bob
      </button>
      <button
        data-testid="btn-id-swap-to-a"
        onClick={() => setId(SEED.users.A)}
      >
        Swap to Alice
      </button>
    </div>
  );
});
