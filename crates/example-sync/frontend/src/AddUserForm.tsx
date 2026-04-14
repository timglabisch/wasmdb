import { useState, type FormEvent } from 'react';
import type { SyncResult, UserCommand } from './sync.ts';

interface Props {
  nextId: () => number;
  execute: (cmd: UserCommand) => Promise<SyncResult>;
}

export default function AddUserForm({ nextId, execute }: Props) {
  const [name, setName] = useState('');
  const [age, setAge] = useState('');

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    if (!name.trim()) return;

    const result = await execute({
      type: 'Insert',
      id: nextId(),
      name: name.trim(),
      age: parseInt(age) || 0,
    });

    if (result.status === 'confirmed') {
      setName('');
      setAge('');
    }
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
