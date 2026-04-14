import { useQuery, useQueryConfirmed } from './sync.ts';

interface User {
  id: number;
  name: string;
  age: number;
  sync: 'pending' | 'confirmed';
}

export default function UsersTable() {
  const rows = useQuery(
    "SELECT users.id, users.name, users.age FROM users",
    ([id, name, age]) => ({ id: id as number, name: name as string, age: age as number }),
  );
  const confirmedIds = new Set(
    useQueryConfirmed("SELECT users.id FROM users", ([id]) => id as number),
  );

  const users: User[] = rows.map(u => ({
    ...u,
    sync: confirmedIds.has(u.id) ? 'confirmed' as const : 'pending' as const,
  }));

  return (
    <table>
      <thead>
        <tr><th>ID</th><th>Name</th><th>Age</th><th>Sync</th></tr>
      </thead>
      <tbody>
        {users.length === 0 ? (
          <tr><td colSpan={4} className="empty">no users yet</td></tr>
        ) : (
          users.map(u => (
            <tr key={u.id}>
              <td>{u.id}</td>
              <td>{u.name}</td>
              <td>{u.age}</td>
              <td className={`sync-${u.sync}`}>{u.sync}</td>
            </tr>
          ))
        )}
      </tbody>
    </table>
  );
}
