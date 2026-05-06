import { memo } from 'react';
import { useQuery } from '@wasmdb/client';
import { useRenderCount, useRenderFlash } from '@wasmdb/scenarios';

interface UserRow {
  id: string;
  name: string;
}

/**
 * Filtered list: only users with `status = 'online'`. Tests
 * boundary-crossing reactivity — when a user transitions in/out of the
 * filter predicate, this list must re-render. Per-row `<UserBadge>`
 * components elsewhere stay quiet (their rows didn't change beyond the
 * status column they don't subscribe to via this list).
 */
export const OnlineUserList = memo(function OnlineUserList() {
  const renders = useRenderCount('OnlineUserList');
  const flashRef = useRenderFlash<HTMLElement>();
  const rows = useQuery<UserRow>(
    `SELECT users.id, users.name FROM users WHERE REACTIVE(users.status = 'online') ORDER BY users.name`,
    ([uid, name]) => ({ id: uid as string, name: name as string }),
  );
  return (
    <section ref={flashRef} className="panel">
      <h2>Online users <small>(r:{renders})</small></h2>
      <ul data-testid="online-user-list">
        {rows.map((u) => (
          <li key={u.id} data-testid={`online-user-${u.id}`}>{u.name}</li>
        ))}
      </ul>
    </section>
  );
});
