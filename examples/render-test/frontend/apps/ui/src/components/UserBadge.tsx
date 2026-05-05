import { memo } from 'react';
import { useQuery } from '@wasmdb/client';
import { useRenderCount } from '../test-utils/useRenderCount';

interface UserRow {
  id: string;
  name: string;
  status: string;
}

interface Props {
  id: string;
  /**
   * Caller-provided context tag (e.g. `room:R1`). Render counts are tracked
   * per-instance so two `<UserBadge id=A>` instances appear as distinct
   * entries in the render-log and don't conflate.
   */
  ctx?: string;
}

export const UserBadge = memo(function UserBadge({ id, ctx }: Props) {
  const tag = ctx ? `UserBadge:${id}@${ctx}` : `UserBadge:${id}`;
  const renders = useRenderCount(tag);
  const rows = useQuery<UserRow>(
    `SELECT users.id, users.name, users.status FROM users WHERE REACTIVE(users.id = UUID '${id}')`,
    ([uid, name, status]) => ({ id: uid as string, name: name as string, status: status as string }),
  );
  const row = rows[0];
  if (!row) return <span data-testid={`user-badge-missing-${id}`}>?</span>;
  return (
    <span data-testid={`user-badge-${id}${ctx ? `-${ctx}` : ''}`} className={`badge status-${row.status}`}>
      <span className="name">{row.name}</span>
      <span className="status">[{row.status}]</span>
      <span className="renders" data-testid={`user-badge-renders-${id}${ctx ? `-${ctx}` : ''}`}>r:{renders}</span>
    </span>
  );
});
