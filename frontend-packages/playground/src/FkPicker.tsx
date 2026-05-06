import { useQuery } from '@wasmdb/client';
import { useRenderFlash } from './hooks';
import { EditableSelect } from './EditableSelect';
import type { FkResolver, Option } from './types';

const DEFAULT_TO_OPTION = (raw: unknown[]): Option => ({
  value: String(raw[1] ?? ''),
  label: String(raw[2] ?? raw[1] ?? ''),
});

/**
 * Generic FK picker. The `resolver` provides the SQL + row→Option mapping for
 * a given `ref` (e.g. "users", "rooms"). Subscribes table-wide so the
 * dropdown labels stay live when referenced rows mutate. Has its own flash
 * ring on the wrapper — pulses whenever the picker re-renders, which is the
 * honest visualization of the subscription it owns.
 */
export function FkPicker({
  resolver,
  value,
  onSave,
  testid,
}: {
  resolver: FkResolver;
  value: string;
  onSave: (id: string) => void;
  testid?: string;
}) {
  const flashRef = useRenderFlash<HTMLDivElement>();
  const toOption = resolver.toOption ?? DEFAULT_TO_OPTION;
  const options = useQuery<Option>(resolver.query, toOption);
  return (
    <div ref={flashRef} className="explorer-picker">
      <EditableSelect
        value={value}
        options={options}
        onSave={onSave}
        testid={testid}
      />
    </div>
  );
}
