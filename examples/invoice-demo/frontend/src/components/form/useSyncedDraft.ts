import { useEffect, useRef, useState } from 'react';

/**
 * Keeps an editable draft in sync with a reactive source value. While the user
 * is editing (dirty), external source changes are ignored. When clean, the
 * source overwrites the draft. Returns [draft, setDraft, dirty, reset].
 */
export function useSyncedDraft<T>(source: T): [T, (next: T) => void, boolean, () => void] {
  const [draft, setDraft] = useState<T>(source);
  const [dirty, setDirty] = useState(false);
  const srcRef = useRef(source);
  useEffect(() => {
    if (srcRef.current !== source) {
      srcRef.current = source;
      if (!dirty) setDraft(source);
    }
  }, [source, dirty]);
  const set = (next: T) => {
    setDraft(next);
    setDirty(next !== source);
  };
  const reset = () => {
    setDraft(source);
    setDirty(false);
  };
  return [draft, set, dirty, reset];
}
