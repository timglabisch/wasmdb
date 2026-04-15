import { useState, type FormEvent } from 'react';
import { useQuery, useQueryConfirmed, execute, nextId } from './sync.ts';

interface OrderRow {
  id: number;
  userId: number;
  userName: string;
  amount: number;
  status: string;
  sync: 'pending' | 'confirmed';
}

const STATUSES = ['pending', 'shipped', 'delivered'];

export default function OrdersPanel() {
  const [userId, setUserId] = useState('');
  const [amount, setAmount] = useState('');
  const [status, setStatus] = useState('pending');

  const userList = useQuery(
    "SELECT users.id, users.name FROM users ORDER BY users.name",
    ([id, name]) => ({ id: id as number, name: name as string }),
  );

  const rows = useQuery(
    "SELECT orders.id, orders.user_id, users.name, orders.amount, orders.status FROM orders INNER JOIN users ON orders.user_id = users.id ORDER BY orders.id",
    ([id, userId, userName, amount, status]) => ({
      id: id as number,
      userId: userId as number,
      userName: userName as string,
      amount: amount as number,
      status: status as string,
    }),
  );

  const confirmedIds = new Set(
    useQueryConfirmed("SELECT orders.id FROM orders", ([id]) => id as number),
  );

  const orders: OrderRow[] = rows.map(o => ({
    ...o,
    sync: confirmedIds.has(o.id) ? 'confirmed' : 'pending',
  }));

  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();
    const uid = parseInt(userId);
    if (!uid || !amount) return;
    const id = nextId();
    const a = parseInt(amount) || 0;
    const s = status.replace(/'/g, "''");
    execute({ type: 'Sql', sql: `INSERT INTO orders VALUES (${id}, ${uid}, ${a}, '${s}')` });
    setAmount('');
  };

  const handleDelete = (id: number) => {
    execute({ type: 'Sql', sql: `DELETE FROM orders WHERE orders.id = ${id}` });
  };

  return (
    <div className="panel">
      <h2>Orders</h2>
      <form className="form" onSubmit={handleSubmit}>
        <select value={userId} onChange={e => setUserId(e.target.value)} className="select-input">
          <option value="">Select user...</option>
          {userList.map(u => <option key={u.id} value={u.id}>{u.name} (#{u.id})</option>)}
        </select>
        <input type="number" value={amount} onChange={e => setAmount(e.target.value)} placeholder="Amount (cents)" />
        <select value={status} onChange={e => setStatus(e.target.value)} className="select-input">
          {STATUSES.map(s => <option key={s} value={s}>{s}</option>)}
        </select>
        <button type="submit">Add Order</button>
      </form>
      <table>
        <thead>
          <tr><th>ID</th><th>User</th><th>Amount</th><th>Status</th><th>Sync</th><th></th></tr>
        </thead>
        <tbody>
          {orders.length === 0 ? (
            <tr><td colSpan={6} className="empty">no orders yet</td></tr>
          ) : orders.map(o => (
            <tr key={o.id}>
              <td>{o.id}</td>
              <td>{o.userName} (#{o.userId})</td>
              <td>${(o.amount / 100).toFixed(2)}</td>
              <td>{o.status}</td>
              <td className={`sync-${o.sync}`}>{o.sync}</td>
              <td><button onClick={() => handleDelete(o.id)} className="btn-sm btn-delete">Del</button></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
