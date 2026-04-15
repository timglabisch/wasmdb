import { useState, type FormEvent } from 'react';
import { execute, executeOnStream, createStream, flushStream, nextId } from './sync.ts';

export default function AddUserForm() {
  const [name, setName] = useState('');
  const [age, setAge] = useState('');

  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();
    if (!name.trim()) return;

    const { confirmed } = execute({
      type: 'Insert',
      id: nextId(),
      name: name.trim(),
      age: parseInt(age) || 0,
    });

    setName('');
    setAge('');

    confirmed
      .then(r => {
        if (r.status === 'rejected') {
          console.error('Command rejected:', r.reason);
        }
      })
      .catch(e => console.error('Command failed:', e));
  };

  const addBulk = async () => {
    const stream = createStream(100); // batch up to 100 commands per request
    for (let i = 0; i < 100; i++) {
      executeOnStream(stream, {
        type: 'Insert',
        id: nextId(),
        name: `User ${Math.random().toString(36).slice(2, 8)}`,
        age: 18 + Math.floor(Math.random() * 50),
      });
    }
    await flushStream(stream); // one HTTP request with all 100 commands
  };

  return (
    <form className="form" onSubmit={handleSubmit}>
      <input
        type="text"
        value={name}
        onChange={e => setName(e.target.value)}
        placeholder="Name"
        autoFocus
      />
      <input
        type="number"
        value={age}
        onChange={e => setAge(e.target.value)}
        placeholder="Age"
      />
      <button type="submit">Add User</button>
      <button type="button" onClick={addBulk}>+100 Users</button>
    </form>
  );
}
