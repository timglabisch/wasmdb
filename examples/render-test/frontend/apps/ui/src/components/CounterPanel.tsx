import { memo } from 'react';
import { useQuery } from '@wasmdb/client';
import { useRenderCount } from '../test-utils/useRenderCount';
import { useRenderFlash } from '../test-utils/useRenderFlash';

interface CounterRow {
  id: string;
  label: string;
  value: number;
}

interface Props {
  id: string;
}

/**
 * Single-row reactive counter. Tests: a `SetCounterValue(id=X)` must
 * re-render only `<Counter:X>`, not other counters.
 *
 * `memo` is intentional — without it, the parent's re-render cascades
 * regardless of whether the underlying row changed.
 */
export const Counter = memo(function Counter({ id }: Props) {
  const renders = useRenderCount(`Counter:${id}`);
  const flashRef = useRenderFlash<HTMLDivElement>();
  const rows = useQuery<CounterRow>(
    `SELECT counters.id, counters.label, counters.value FROM counters WHERE REACTIVE(counters.id = UUID '${id}')`,
    ([cid, label, value]) => ({ id: cid as string, label: label as string, value: value as number }),
  );
  const row = rows[0];
  if (!row) return null;
  return (
    <div ref={flashRef} data-testid={`counter-${id}`} className="counter">
      <span className="label">{row.label}</span>
      <span className="value" data-testid={`counter-value-${id}`}>{row.value}</span>
      <span className="renders" data-testid={`counter-renders-${id}`}>renders: {renders}</span>
    </div>
  );
});

export function CounterPanel({ ids }: { ids: string[] }) {
  const renders = useRenderCount('CounterPanel');
  return (
    <section className="panel">
      <h2>Counters <small>(parent renders: {renders})</small></h2>
      <div className="grid">
        {ids.map((id) => (
          <Counter key={id} id={id} />
        ))}
      </div>
    </section>
  );
}
