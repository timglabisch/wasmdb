import { useState, type FormEvent } from 'react';
import { execute, nextId } from './sync.ts';

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
    </form>
  );
}
