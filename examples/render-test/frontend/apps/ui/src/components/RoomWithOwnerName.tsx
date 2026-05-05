import { memo } from 'react';
import { useQuery } from '@wasmdb/client';
import { useRenderCount } from '../test-utils/useRenderCount';

interface Row {
  roomName: string;
  ownerName: string;
}

interface Props {
  roomId: string;
}

/**
 * Reactive JOIN: room ⨝ users on `owner_user_id`. The single query reads
 * a column from each table — both sides must be reactive sources so that
 * either a `RenameRoom(roomId)` *or* `UpdateUserName(owner)` re-fires.
 */
export const RoomWithOwnerName = memo(function RoomWithOwnerName({ roomId }: Props) {
  const renders = useRenderCount(`RoomWithOwnerName:${roomId}`);
  const rows = useQuery<Row>(
    `SELECT REACTIVE(users.id), rooms.name, users.name FROM rooms JOIN users ON users.id = rooms.owner_user_id WHERE REACTIVE(rooms.id = UUID '${roomId}')`,
    ([_uid, rn, un]) => ({ roomName: rn as string, ownerName: un as string }),
  );
  const row = rows[0];
  if (!row) return null;
  return (
    <div data-testid={`room-owner-${roomId}`} className="room-owner-line">
      <span>{row.roomName} — owned by {row.ownerName}</span>
      <span className="renders">r:{renders}</span>
    </div>
  );
});
