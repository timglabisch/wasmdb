import { useAction } from './ActionTracker';

export type TrackedButtonVariant = 'default' | 'danger' | 'positive';

interface Props {
  testId: string;
  label: string;
  action: () => void;
  variant?: TrackedButtonVariant;
}

/**
 * Tracked button. Wraps the user-supplied action in a snapshot/diff
 * roundtrip so the live diff panel updates after every click.
 */
export function TrackedButton({ testId, label, action, variant = 'default' }: Props) {
  const { track } = useAction();
  return (
    <button
      data-testid={testId}
      className={`tracked-btn variant-${variant}`}
      onClick={() => track(label, action)}
    >
      {label}
    </button>
  );
}
