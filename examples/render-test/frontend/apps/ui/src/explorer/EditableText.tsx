import { useState, type KeyboardEvent } from 'react';

interface Props {
  value: string;
  onSave?: (next: string) => void;
  testid?: string;
  placeholder?: string;
  readOnly?: boolean;
  monospace?: boolean;
}

/**
 * Click-to-edit text cell. Commits on blur or Enter; cancels on Escape.
 * Skips the command call if the value didn't change.
 */
export function EditableText({ value, onSave, testid, placeholder, readOnly, monospace }: Props) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(value);

  if (readOnly || !editing) {
    return (
      <span
        className={`editable-display${readOnly ? ' editable-readonly' : ''}${monospace ? ' editable-mono' : ''}`}
        data-testid={testid}
        title={readOnly ? 'read-only (no command exposed)' : 'click to edit'}
        onClick={readOnly ? undefined : () => { setDraft(value); setEditing(true); }}
      >
        {value || <em className="editable-empty">{placeholder ?? '(empty)'}</em>}
      </span>
    );
  }

  const commit = () => {
    setEditing(false);
    if (draft !== value && onSave) onSave(draft);
  };
  const cancel = () => { setEditing(false); setDraft(value); };
  const onKey = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') commit();
    if (e.key === 'Escape') cancel();
  };

  return (
    <input
      autoFocus
      className={`editable-input${monospace ? ' editable-mono' : ''}`}
      data-testid={testid}
      value={draft}
      onChange={(e) => setDraft(e.target.value)}
      onBlur={commit}
      onKeyDown={onKey}
    />
  );
}
