import { memo } from 'react';
import { useQuery } from '@wasmdb/client';
import { useRenderCount, useRenderFlash } from '@wasmdb/scenarios';
import { UserBadge } from './UserBadge';

interface RoomRowData {
  id: string;
  name: string;
  owner_user_id: string;
}

interface RowProps {
  id: string;
}

/**
 * Per-room row reads only its own row. Cross-table reactivity comes from
 * the embedded `<UserBadge>` which subscribes independently. The room row
 * itself does NOT re-read `users` — testing that `updateUserName(A)`
 * leaves `<RoomRow>` quiet (only `<UserBadge:A>` re-renders) and
 * `transferRoom(R1, B)` re-renders `<RoomRow:R1>` (its own row changed)
 * but leaves `<UserBadge:A>` alone.
 */
export const RoomRow = memo(function RoomRow({ id }: RowProps) {
  const renders = useRenderCount(`RoomRow:${id}`);
  const flashRef = useRenderFlash<HTMLLIElement>();
  const rows = useQuery<RoomRowData>(
    `SELECT rooms.id, rooms.name, rooms.owner_user_id FROM rooms WHERE REACTIVE(rooms.id = UUID '${id}')`,
    ([rid, name, owner_user_id]) => ({
      id: rid as string,
      name: name as string,
      owner_user_id: owner_user_id as string,
    }),
  );
  const row = rows[0];
  if (!row) return null;
  return (
    <li ref={flashRef} data-testid={`room-row-${id}`} className="room-row">
      <span className="room-name">{row.name}</span>
      <span className="room-owner">owner: <UserBadge id={row.owner_user_id} ctx={`room:${id}`} /></span>
      <span className="renders" data-testid={`room-renders-${id}`}>r:{renders}</span>
    </li>
  );
});

export function RoomList() {
  const renders = useRenderCount('RoomList');
  const flashRef = useRenderFlash<HTMLElement>();
  const rooms = useQuery<{ id: string }>(
    'SELECT REACTIVE(rooms.id), rooms.id FROM rooms ORDER BY rooms.name',
    ([_r, rid]) => ({ id: rid as string }),
  );
  // ↑ REACTIVE(col) in SELECT is *table-wide* — fires on any rooms change.
  // That's exactly what we want for the list (insert/delete/rename order).
  return (
    <section ref={flashRef} className="panel">
      <h2>Rooms <small>(parent renders: {renders})</small></h2>
      <ul className="room-list">
        {rooms.map((r) => <RoomRow key={r.id} id={r.id} />)}
      </ul>
    </section>
  );
}
