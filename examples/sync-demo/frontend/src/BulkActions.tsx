import { useQuery, createStream, flushStream } from '@wasmdb/client';
import { execute, executeOnStream, nextId } from './commands.ts';
export default function BulkActions() {
  const userIds = useQuery("SELECT reactive(users.id), users.id FROM users", ([_r, id]) => id as number);
  const orderIds = useQuery("SELECT reactive(orders.id), orders.id FROM orders", ([_r, id]) => id as number);
  const users = useQuery(
    "SELECT reactive(users.id), users.id, users.name FROM users",
    ([_r, id, name]) => ({ id: id as number, name: name as string }),
  );

  const addBulkUsers = async (count: number) => {
    const stream = createStream(count);
    for (let i = 0; i < count; i++) {
      const id = nextId();
      const name = `User-${Math.random().toString(36).slice(2, 8)}`;
      const age = 18 + Math.floor(Math.random() * 50);
      executeOnStream(stream, { type: 'InsertUser', id, name, age });
    }
    await flushStream(stream);
  };

  const addBulkOrders = async (count: number) => {
    const stream = createStream(count);
    for (let i = 0; i < count; i++) {
      const id = nextId();
      const userId = 1 + Math.floor(Math.random() * 10);
      const amount = 100 + Math.floor(Math.random() * 9900);
      const status = ['pending', 'shipped', 'delivered'][Math.floor(Math.random() * 3)];
      executeOnStream(stream, { type: 'InsertOrder', id, user_id: userId, amount, status });
    }
    await flushStream(stream);
  };

  const bulkUpdateAges = async () => {
    if (users.length === 0) return;
    const stream = createStream(users.length);
    for (const user of users) {
      const newAge = 20 + Math.floor(Math.random() * 40);
      executeOnStream(stream, { type: 'UpdateUser', id: user.id, name: user.name, age: newAge });
    }
    await flushStream(stream);
  };

  const clearAll = () => {
    if (orderIds.length > 0) execute({ type: 'DeleteOrders', ids: orderIds });
    if (userIds.length > 0) execute({ type: 'DeleteUsers', ids: userIds });
  };

  return (
    <div className="bulk-actions">
      <h3>Bulk Operations</h3>
      <div className="bulk-buttons">
        <button onClick={() => addBulkUsers(100)}>+100 Users</button>
        <button onClick={() => addBulkOrders(100)}>+100 Orders</button>
        <button onClick={() => addBulkUsers(500)}>+500 Users</button>
        <button onClick={() => addBulkOrders(500)}>+500 Orders</button>
        <button onClick={bulkUpdateAges} className="btn-warn">Bulk Update Ages</button>
        <button onClick={clearAll} className="btn-danger">Clear All</button>
      </div>
    </div>
  );
}
