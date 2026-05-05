import { memo } from 'react';
import { useQuery } from '@wasmdb/client';
import { useRenderCount } from '../test-utils/useRenderCount';

interface Props {
  roomId: string;
}

/**
 * Aggregate query (`COUNT(*)`) over a per-room slice. Verifies that
 * aggregate-style reactivity is bounded to the table-slice that drives
 * it — `AddMessage(R1)` re-renders `MessageCount:R1`, leaves
 * `MessageCount:R2/R3` quiet.
 */
export const MessageCount = memo(function MessageCount({ roomId }: Props) {
  const renders = useRenderCount(`MessageCount:${roomId}`);
  const rows = useQuery<{ n: number }>(
    `SELECT COUNT(messages.id) FROM messages WHERE REACTIVE(messages.room_id = UUID '${roomId}')`,
    ([n]) => ({ n: n as number }),
  );
  const n = rows[0]?.n ?? 0;
  return (
    <span data-testid={`message-count-${roomId}`} className="message-count">
      <span data-testid={`message-count-value-${roomId}`}>{n}</span>
      <span className="renders">r:{renders}</span>
    </span>
  );
});
