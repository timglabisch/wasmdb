import { memo } from 'react';
import { useQuery } from '@wasmdb/client';
import { useRenderCount } from '../test-utils/useRenderCount';
import { useRenderFlash } from '../test-utils/useRenderFlash';
import { UserBadge } from './UserBadge';

interface MessageRow {
  id: string;
  body: string;
  author_user_id: string;
  created_at: string;
}

const MessageItem = memo(function MessageItem({ id }: { id: string }) {
  const renders = useRenderCount(`MessageItem:${id}`);
  const flashRef = useRenderFlash<HTMLLIElement>();
  const rows = useQuery<MessageRow>(
    `SELECT messages.id, messages.body, messages.author_user_id, messages.created_at FROM messages WHERE REACTIVE(messages.id = UUID '${id}')`,
    ([mid, body, author_user_id, created_at]) => ({
      id: mid as string,
      body: body as string,
      author_user_id: author_user_id as string,
      created_at: created_at as string,
    }),
  );
  const row = rows[0];
  if (!row) return null;
  return (
    <li ref={flashRef} data-testid={`message-${id}`} className="message-item">
      <UserBadge id={row.author_user_id} ctx={`msg:${id}`} />
      <span className="body">{row.body}</span>
      <span className="renders" data-testid={`message-renders-${id}`}>r:{renders}</span>
    </li>
  );
});

interface Props {
  roomId: string;
}

/**
 * Per-room messages list. Tests list-membership reactivity: `addMessage`
 * to room R1 must re-render `<MessageList:R1>` (the list query saw a new
 * row) but NOT `<MessageList:R2>`. Existing `<MessageItem>`s in R1 should
 * stay quiet (their rows didn't change).
 */
export const MessageList = memo(function MessageList({ roomId }: Props) {
  const renders = useRenderCount(`MessageList:${roomId}`);
  const flashRef = useRenderFlash<HTMLDivElement>();
  const ids = useQuery<{ id: string }>(
    `SELECT messages.id FROM messages WHERE REACTIVE(messages.room_id = UUID '${roomId}') ORDER BY messages.created_at`,
    ([mid]) => ({ id: mid as string }),
  );
  return (
    <div ref={flashRef} data-testid={`message-list-${roomId}`} className="message-list">
      <h3>Messages <small>(r:{renders})</small></h3>
      <ul>
        {ids.map((m) => <MessageItem key={m.id} id={m.id} />)}
      </ul>
    </div>
  );
});
