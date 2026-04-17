import { useState, useCallback, memo, type FormEvent } from 'react';
import { useQuery, useQueryConfirmed, execute, nextId } from './sync.ts';

interface UserRow {
  id: number;
  name: string;
  age: number;
  orderCount: number;
  sync: 'pending' | 'confirmed';
}

const UserRowView = memo(function UserRowView({
  user,
  onDelete,
  onStartEdit,
}: {
  user: UserRow;
  onDelete: (id: number) => void;
  onStartEdit: (id: number) => void;
}) {
  return (
    <tr>
      <td>{user.id}</td>
      <td>{user.name}</td>
      <td>{user.age}</td>
      <td>{user.orderCount}</td>
      <td className={`sync-${user.sync}`}>{user.sync}</td>
      <td className="actions">
        <button onClick={() => onStartEdit(user.id)} className="btn-sm btn-edit">Edit</button>
        <button onClick={() => onDelete(user.id)} className="btn-sm btn-delete">Del</button>
      </td>
    </tr>
  );
}, (prev, next) =>
  prev.user.id === next.user.id &&
  prev.user.name === next.user.name &&
  prev.user.age === next.user.age &&
  prev.user.orderCount === next.user.orderCount &&
  prev.user.sync === next.user.sync
);

export default function UsersPanel() {
  const [name, setName] = useState('');
  const [age, setAge] = useState('');
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editName, setEditName] = useState('');
  const [editAge, setEditAge] = useState('');

  const rows = useQuery(
    "SELECT reactive(users.id), reactive(orders.id), users.id, users.name, users.age, COUNT(orders.id) FROM users LEFT JOIN orders ON users.id = orders.user_id GROUP BY users.id, users.name, users.age ORDER BY users.id",
    ([_r1, _r2, id, name, age, orderCount]) => ({
      id: id as number,
      name: name as string,
      age: age as number,
      orderCount: orderCount as number,
    }),
  );

  const confirmedIds = new Set(
    useQueryConfirmed("SELECT reactive(users.id), users.id FROM users", ([_r, id]) => id as number),
  );

  const users: UserRow[] = rows.map(u => ({
    ...u,
    sync: confirmedIds.has(u.id) ? 'confirmed' : 'pending',
  }));

  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();
    if (!name.trim()) return;
    const id = nextId();
    const a = parseInt(age) || 0;
    execute({ type: 'InsertUser', id, name: name.trim(), age: a });
    setName('');
    setAge('');
  };

  const handleDelete = useCallback((id: number) => {
    execute({ type: 'DeleteUsers', ids: [id] });
  }, []);

  const startEdit = (u: UserRow) => {
    setEditingId(u.id);
    setEditName(u.name);
    setEditAge(String(u.age));
  };

  const handleStartEdit = useCallback((id: number) => {
    const u = users.find(u => u.id === id);
    if (u) startEdit(u);
  }, [users]);

  const saveEdit = () => {
    if (editingId === null) return;
    const a = parseInt(editAge) || 0;
    execute({ type: 'UpdateUser', id: editingId, name: editName.trim(), age: a });
    setEditingId(null);
  };

  const cancelEdit = () => setEditingId(null);

  return (
    <div className="panel">
      <h2>Users</h2>
      <form className="form" onSubmit={handleSubmit}>
        <input type="text" value={name} onChange={e => setName(e.target.value)} placeholder="Name" />
        <input type="number" value={age} onChange={e => setAge(e.target.value)} placeholder="Age" />
        <button type="submit">Add</button>
      </form>
      <table className="table-users">
        <colgroup>
          <col /><col /><col /><col /><col /><col />
        </colgroup>
        <thead>
          <tr><th>ID</th><th>Name</th><th>Age</th><th>Orders</th><th>Sync</th><th></th></tr>
        </thead>
        <tbody>
          {users.length === 0 ? (
            <tr><td colSpan={6} className="empty">no users yet</td></tr>
          ) : users.map(u => (
            editingId === u.id ? (
              <tr key={u.id}>
                <td>{u.id}</td>
                <td><input value={editName} onChange={e => setEditName(e.target.value)} className="inline-edit" /></td>
                <td><input type="number" value={editAge} onChange={e => setEditAge(e.target.value)} className="inline-edit inline-edit-sm" /></td>
                <td>{u.orderCount}</td>
                <td className={`sync-${u.sync}`}>{u.sync}</td>
                <td className="actions">
                  <button onClick={saveEdit} className="btn-sm btn-save">Save</button>
                  <button onClick={cancelEdit} className="btn-sm">Cancel</button>
                </td>
              </tr>
            ) : (
              <UserRowView key={u.id} user={u} onDelete={handleDelete} onStartEdit={handleStartEdit} />
            )
          ))}
        </tbody>
      </table>
    </div>
  );
}
