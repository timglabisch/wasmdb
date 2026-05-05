import { memo, useState } from 'react';
import { MessageList } from './MessageList';
import { SEED } from '../seed';

/**
 * Wrapper that toggles a `<MessageList:R1>` in/out of the tree. The spec
 * uses this to test that unmounting a subscriber while a command is
 * mid-flight is safe: no crash, no late renders to a torn-down component.
 */
export const UnmountProbe = memo(function UnmountProbe() {
  const [shown, setShown] = useState(true);
  return (
    <div className="unmount-probe">
      <button
        data-testid="btn-toggle-unmount-r1"
        onClick={() => setShown((s) => !s)}
      >
        {shown ? 'Hide' : 'Show'} R1 (probe)
      </button>
      {shown && (
        <div data-testid="unmount-probe-mounted">
          <MessageList roomId={SEED.rooms.R1} />
        </div>
      )}
    </div>
  );
});
