interface Option {
  value: string;
  label: string;
}

interface Props {
  value: string;
  options: Option[];
  onSave: (next: string) => void;
  testid?: string;
}

/**
 * Native <select>. Direct change → commit. If the current value isn't in the
 * known options (e.g. dangling FK), it's still shown as a fallback option so
 * the user can see the raw id.
 */
export function EditableSelect({ value, options, onSave, testid }: Props) {
  const known = options.find((o) => o.value === value);
  return (
    <select
      className="editable-select"
      data-testid={testid}
      value={value}
      onChange={(e) => { if (e.target.value !== value) onSave(e.target.value); }}
    >
      {!known && (
        <option value={value}>{`(unknown ${value.slice(-4)})`}</option>
      )}
      {options.map((o) => (
        <option key={o.value} value={o.value}>{o.label}</option>
      ))}
    </select>
  );
}
