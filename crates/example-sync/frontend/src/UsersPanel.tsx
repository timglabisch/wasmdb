import { useState, type FormEvent } from 'react';
import { useQuery, useQueryConfirmed, execute, nextId } from './sync.ts';

interface UserRow {
  id: number;
  name: string;
  age: number;
  orderCount: number;
  sync: 'pending' | 'confirmed';
}

export default function UsersPanel() {
  const [name, setName] = useState('');
  const [age, setAge] = useState('');
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editName, setEditName] = useState('');
  const [editAge, setEditAge] = useState('');

  const rows = useQuery(
    "SELECT users.id, users.name, users.age, COUNT(orders.id) FROM users LEFT JOIN orders ON users.id = orders.user_id GROUP BY users.id, users.name, users.age ORDER BY users.id",
    ([id, name, age, orderCount]) => ({
      id: id as number,
      name: name as string,
      age: age as number,
      orderCount: orderCount as number,
    }),
  );

  const confirmedIds = new Set(
    useQueryConfirmed("SELECT users.id FROM users", ([id]) => id as number),
  );

  const users: UserRow[] = rows.map(u => ({
    ...u,
    sync: confirmedIds.has(u.id) ? 'confirmed' : 'pending',
  }));

  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();
    if (!name.trim()) return;
    const id = nextId();
    const n = name.trim().replace(/'/g, "''");
    const a = parseInt(age) || 0;
    execute({ type: 'Sql', sql: `INSERT INTO users VALUES (${id}, '${n}', ${a})` });
    setName('');
    setAge('');
  };

  const handleDelete = (id: number) => {
    execute({ type: 'Sql', sql: `DELETE FROM users WHERE users.id = ${id}` });
  };

  const startEdit = (u: UserRow) => {
    setEditingId(u.id);
    setEditName(u.name);
    setEditAge(String(u.age));
  };

  const saveEdit = () => {
    if (editingId === null) return;
    const n = editName.trim().replace(/'/g, "''");
    const a = parseInt(editAge) || 0;
    execute({ type: 'Sql', sql: `UPDATE users SET name = '${n}', age = ${a} WHERE users.id = ${editingId}` });
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
      <table>
        <thead>
          <tr><th>ID</th><th>Name</th><th>Age</th><th>Orders</th><th>Sync</th><th></th></tr>
        </thead>
        <tbody>
          {users.length === 0 ? (
            <tr><td colSpan={6} className="empty">no users yet</td></tr>
          ) : users.map(u => (
            <tr key={u.id}>
              <td>{u.id}</td>
              <td>
                {editingId === u.id
                  ? <input value={editName} onChange={e => setEditName(e.target.value)} className="inline-edit" />
                  : u.name}
              </td>
              <td>
                {editingId === u.id
                  ? <input type="number" value={editAge} onChange={e => setEditAge(e.target.value)} className="inline-edit inline-edit-sm" />
                  : u.age}
              </td>
              <td>{u.orderCount}</td>
              <td className={`sync-${u.sync}`}>{u.sync}</td>
              <td className="actions">
                {editingId === u.id ? (
                  <>
                    <button onClick={saveEdit} className="btn-sm btn-save">Save</button>
                    <button onClick={cancelEdit} className="btn-sm">Cancel</button>
                  </>
                ) : (
                  <>
                    <button onClick={() => startEdit(u)} className="btn-sm btn-edit">Edit</button>
                    <button onClick={() => handleDelete(u.id)} className="btn-sm btn-delete">Del</button>
                  </>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
