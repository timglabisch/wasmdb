import { useState } from 'react';
import { execute, executeOnStream, createStream, flushStream, nextId } from '../../sync';

interface ScenarioResult {
  name: string;
  status: 'running' | 'done' | 'error';
  message?: string;
}

export function ScenarioPanel() {
  const [results, setResults] = useState<ScenarioResult[]>([]);

  const log = (r: ScenarioResult) =>
    setResults(prev => [r, ...prev].slice(0, 50));

  const insertDuplicate = async () => {
    // First insert a user, then try inserting with the same ID
    const id = nextId();
    const { confirmed: first } = execute({ type: 'Insert', id, name: 'Original', age: 25 });
    await first;
    log({ name: 'Insert Duplicate', status: 'running' });
    const { confirmed } = execute({ type: 'Insert', id, name: 'Duplicate', age: 30 });
    const r = await confirmed;
    log({
      name: 'Insert Duplicate',
      status: r.status === 'rejected' ? 'done' : 'error',
      message: r.status === 'rejected' ? `Rejected: ${r.reason}` : 'Expected rejection but got confirmed!',
    });
  };

  const deleteRandom = async () => {
    log({ name: 'Delete Random', status: 'running' });
    // Insert a user, then delete it
    const id = nextId();
    const { confirmed: insertOk } = execute({ type: 'Insert', id, name: `ToDelete-${id}`, age: 99 });
    await insertOk;
    const { confirmed } = execute({ type: 'Delete', id });
    const r = await confirmed;
    log({
      name: 'Delete Random',
      status: r.status === 'confirmed' ? 'done' : 'error',
      message: r.status === 'confirmed' ? `Deleted user ${id}` : `Failed: ${r.reason}`,
    });
  };

  const bulkInsert = async (count: number) => {
    log({ name: `Bulk ${count}`, status: 'running' });
    const stream = createStream(count);
    for (let i = 0; i < count; i++) {
      executeOnStream(stream, {
        type: 'Insert',
        id: nextId(),
        name: `Bulk-${Math.random().toString(36).slice(2, 8)}`,
        age: 18 + Math.floor(Math.random() * 50),
      });
    }
    await flushStream(stream);
    log({ name: `Bulk ${count}`, status: 'done', message: `${count} users inserted` });
  };

  const parallelStreams = async () => {
    log({ name: 'Parallel Streams ×3', status: 'running' });
    const promises: Promise<void>[] = [];
    for (let s = 0; s < 3; s++) {
      const stream = createStream(10);
      for (let i = 0; i < 10; i++) {
        executeOnStream(stream, {
          type: 'Insert',
          id: nextId(),
          name: `S${s}-${Math.random().toString(36).slice(2, 6)}`,
          age: 20 + s * 10 + i,
        });
      }
      promises.push(flushStream(stream));
    }
    await Promise.all(promises);
    log({ name: 'Parallel Streams ×3', status: 'done', message: '30 users across 3 streams' });
  };

  const insertAndDelete = async () => {
    log({ name: 'Insert+Delete Cycle', status: 'running' });
    const ids: number[] = [];
    const stream = createStream(20);
    // Insert 10
    for (let i = 0; i < 10; i++) {
      const id = nextId();
      ids.push(id);
      executeOnStream(stream, {
        type: 'Insert', id, name: `Cycle-${i}`, age: 30 + i,
      });
    }
    // Delete 5 of them
    for (let i = 0; i < 5; i++) {
      executeOnStream(stream, { type: 'Delete', id: ids[i] });
    }
    await flushStream(stream);
    log({ name: 'Insert+Delete Cycle', status: 'done', message: '10 inserted, 5 deleted → net 5' });
  };

  const delayedBatch = async () => {
    log({ name: 'Delayed Batch', status: 'running' });
    const stream = createStream(50, 1000); // batch up to 50, wait 1s
    for (let i = 0; i < 5; i++) {
      executeOnStream(stream, {
        type: 'Insert',
        id: nextId(),
        name: `Delayed-${i}`,
        age: 40 + i,
      });
    }
    // Don't flush — let the timer trigger it
    log({ name: 'Delayed Batch', status: 'done', message: '5 commands queued, waiting for timer flush' });
  };

  return (
    <div className="debug-panel-scenarios">
      <div className="debug-scenario-buttons">
        <button className="debug-btn-scenario" onClick={insertDuplicate}>Insert Duplicate</button>
        <button className="debug-btn-scenario" onClick={deleteRandom}>Delete User</button>
        <button className="debug-btn-scenario" onClick={() => bulkInsert(100)}>Bulk 100</button>
        <button className="debug-btn-scenario" onClick={() => bulkInsert(1000)}>Bulk 1000</button>
        <button className="debug-btn-scenario" onClick={parallelStreams}>Parallel ×3</button>
        <button className="debug-btn-scenario" onClick={insertAndDelete}>Insert+Delete</button>
        <button className="debug-btn-scenario" onClick={delayedBatch}>Delayed Batch</button>
      </div>
      {results.length > 0 && (
        <div className="debug-scenario-log">
          {results.map((r, i) => (
            <div key={i} className={`debug-scenario-entry debug-scenario-${r.status}`}>
              <span className="debug-scenario-name">{r.name}</span>
              <span className="debug-scenario-status">{r.status}</span>
              {r.message && <span className="debug-scenario-msg">{r.message}</span>}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
