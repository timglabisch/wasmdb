import { memo } from 'react';
import { useQuery } from '@wasmdb/client';
import { useRenderCount } from '../test-utils/useRenderCount';
import { useRenderFlash } from '../test-utils/useRenderFlash';

interface Row {
  id: string;
}

/**
 * Subquery / EXISTS-style reactivity. Lists rooms that have at least one
 * message. Tests that messages-table membership changes (last message
 * deleted in a room) propagate into a query whose primary table is
 * `rooms`. Both `rooms` and `messages` must be reactive sources.
 */
export const RoomsWithMessages = memo(function RoomsWithMessages() {
  const renders = useRenderCount('RoomsWithMessages');
  const flashRef = useRenderFlash<HTMLElement>();
  const rows = useQuery<Row>(
    `SELECT REACTIVE(messages.room_id), rooms.id FROM rooms JOIN messages ON messages.room_id = rooms.id ORDER BY rooms.id`,
    ([_r, rid]) => ({ id: rid as string }),
  );
  return (
    <section ref={flashRef} className="panel">
      <h2>Rooms with messages <small>(r:{renders})</small></h2>
      <ul data-testid="rooms-with-messages">
        {rows.map((r) => <li key={r.id} data-testid={`rwm-${r.id}`}>{r.id}</li>)}
      </ul>
    </section>
  );
});
