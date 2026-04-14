import type { User } from './sync.ts';

interface Props {
  users: User[];
}

export default function UsersTable({ users }: Props) {
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
