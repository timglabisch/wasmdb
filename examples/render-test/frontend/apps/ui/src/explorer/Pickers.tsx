import { execute, useQuery } from '@wasmdb/client';
import { useRenderFlash } from '../test-utils/useRenderFlash';
import { EditableSelect } from './EditableSelect';

/**
 * Owner picker. Subscribes table-wide to `users` so the dropdown labels stay
 * live (renaming a user updates options in every picker). Has its own
 * flashRef on the <select> wrapper — it ticks whenever the users table is
 * mutated, which is the *honest* visualization of the subscription it owns.
 */
export function UserPicker({
  value,
  onSave,
  testid,
}: {
  value: string;
  onSave: (id: string) => void;
  testid?: string;
}) {
  const flashRef = useRenderFlash<HTMLDivElement>();
  const users = useQuery<{ id: string; name: string }>(
    'SELECT REACTIVE(users.id), users.id, users.name FROM users ORDER BY users.name',
    ([_r, id, name]) => ({ id: id as string, name: name as string }),
  );
  return (
    <div ref={flashRef} className="explorer-picker">
      <EditableSelect
        value={value}
        options={users.map((u) => ({ value: u.id, label: u.name }))}
        onSave={onSave}
        testid={testid}
      />
    </div>
  );
}

export function RoomPicker({
  value,
  onSave,
  testid,
}: {
  value: string;
  onSave: (id: string) => void;
  testid?: string;
}) {
  const flashRef = useRenderFlash<HTMLDivElement>();
  const rooms = useQuery<{ id: string; name: string }>(
    'SELECT REACTIVE(rooms.id), rooms.id, rooms.name FROM rooms ORDER BY rooms.name',
    ([_r, id, name]) => ({ id: id as string, name: name as string }),
  );
  return (
    <div ref={flashRef} className="explorer-picker">
      <EditableSelect
        value={value}
        options={rooms.map((r) => ({ value: r.id, label: r.name }))}
        onSave={onSave}
        testid={testid}
      />
    </div>
  );
}

/**
 * Helper: fire UpdateUserName / RenameRoom etc. directly so callers don't
 * have to pass shaped commands. Keeps callsites compact.
 */
export const fire = execute;
