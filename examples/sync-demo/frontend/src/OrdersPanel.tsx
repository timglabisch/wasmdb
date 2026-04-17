import { useState, useCallback, memo, type FormEvent } from 'react';
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

const OrderRowView = memo(function OrderRowView({
  order,
  onDelete,
}: {
  order: OrderRow;
  onDelete: (id: number) => void;
}) {
  return (
    <tr>
      <td>{order.id}</td>
      <td>{order.userName} (#{order.userId})</td>
      <td>${(order.amount / 100).toFixed(2)}</td>
      <td>{order.status}</td>
      <td className={`sync-${order.sync}`}>{order.sync}</td>
      <td><button onClick={() => onDelete(order.id)} className="btn-sm btn-delete">Del</button></td>
    </tr>
  );
}, (prev, next) =>
  prev.order.id === next.order.id &&
  prev.order.userId === next.order.userId &&
  prev.order.userName === next.order.userName &&
  prev.order.amount === next.order.amount &&
  prev.order.status === next.order.status &&
  prev.order.sync === next.order.sync
);

export default function OrdersPanel() {
  const [userId, setUserId] = useState('');
  const [amount, setAmount] = useState('');
  const [status, setStatus] = useState('pending');

  const userList = useQuery(
    "SELECT reactive(users.id), users.id, users.name FROM users ORDER BY users.name",
    ([_r, id, name]) => ({ id: id as number, name: name as string }),
  );

  const rows = useQuery(
    "SELECT reactive(orders.id), reactive(users.id), orders.id, orders.user_id, users.name, orders.amount, orders.status FROM orders INNER JOIN users ON orders.user_id = users.id ORDER BY orders.id",
    ([_r1, _r2, id, userId, userName, amount, status]) => ({
      id: id as number,
      userId: userId as number,
      userName: userName as string,
      amount: amount as number,
      status: status as string,
    }),
  );

  const confirmedIds = new Set(
    useQueryConfirmed("SELECT reactive(orders.id), orders.id FROM orders", ([_r, id]) => id as number),
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
    execute({ type: 'InsertOrder', id, user_id: uid, amount: a, status });
    setAmount('');
  };

  const handleDelete = useCallback((id: number) => {
    execute({ type: 'DeleteOrders', ids: [id] });
  }, []);

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
      <table className="table-orders">
        <colgroup>
          <col /><col /><col /><col /><col /><col />
        </colgroup>
        <thead>
          <tr><th>ID</th><th>User</th><th>Amount</th><th>Status</th><th>Sync</th><th></th></tr>
        </thead>
        <tbody>
          {orders.length === 0 ? (
            <tr><td colSpan={6} className="empty">no orders yet</td></tr>
          ) : orders.map(o => (
            <OrderRowView key={o.id} order={o} onDelete={handleDelete} />
          ))}
        </tbody>
      </table>
    </div>
  );
}
