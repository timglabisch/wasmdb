import { useState, type KeyboardEvent } from 'react';

interface Props {
  value: number;
  onSave: (next: number) => void;
  testid?: string;
}

export function EditableNumber({ value, onSave, testid }: Props) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(String(value));

  if (!editing) {
    return (
      <span
        className="editable-display editable-mono"
        data-testid={testid}
        title="click to edit"
        onClick={() => { setDraft(String(value)); setEditing(true); }}
      >
        {value}
      </span>
    );
  }

  const commit = () => {
    setEditing(false);
    const n = Number(draft);
    if (!Number.isFinite(n)) return;
    if (n !== value) onSave(n);
  };
  const cancel = () => { setEditing(false); setDraft(String(value)); };
  const onKey = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') commit();
    if (e.key === 'Escape') cancel();
  };

  return (
    <input
      autoFocus
      type="number"
      className="editable-input editable-mono"
      data-testid={testid}
      value={draft}
      onChange={(e) => setDraft(e.target.value)}
      onBlur={commit}
      onKeyDown={onKey}
    />
  );
}
