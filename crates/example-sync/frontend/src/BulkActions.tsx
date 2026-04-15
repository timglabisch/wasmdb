import { execute, executeOnStream, createStream, flushStream, nextId } from './sync.ts';

export default function BulkActions() {
  const addBulkUsers = async (count: number) => {
    const stream = createStream(count);
    for (let i = 0; i < count; i++) {
      const id = nextId();
      const name = `User-${Math.random().toString(36).slice(2, 8)}`;
      const age = 18 + Math.floor(Math.random() * 50);
      executeOnStream(stream, {
        type: 'Sql',
        sql: `INSERT INTO users VALUES (${id}, '${name}', ${age})`,
      });
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
      executeOnStream(stream, {
        type: 'Sql',
        sql: `INSERT INTO orders VALUES (${id}, ${userId}, ${amount}, '${status}')`,
      });
    }
    await flushStream(stream);
  };

  const bulkUpdateAges = async () => {
    const stream = createStream(50);
    for (let i = 1; i <= 50; i++) {
      const newAge = 20 + Math.floor(Math.random() * 40);
      executeOnStream(stream, {
        type: 'Sql',
        sql: `UPDATE users SET age = ${newAge} WHERE users.id = ${i}`,
      });
    }
    await flushStream(stream);
  };

  const clearAll = () => {
    execute({ type: 'Sql', sql: 'DELETE FROM orders' });
    execute({ type: 'Sql', sql: 'DELETE FROM users' });
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
