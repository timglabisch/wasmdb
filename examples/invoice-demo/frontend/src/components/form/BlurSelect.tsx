import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from '@/components/ui/select';

export interface BlurSelectOption {
  value: string;
  label: string;
}

/**
 * Controlled <Select> that commits on change. Works with string values only;
 * callers convert numeric IDs via String()/Number() at the boundary.
 */
export function BlurSelect({
  value,
  onCommit,
  options,
  placeholder,
  className,
  disabled,
}: {
  value: string;
  onCommit: (next: string) => void;
  options: BlurSelectOption[];
  placeholder?: string;
  className?: string;
  disabled?: boolean;
}) {
  return (
    <Select value={value || undefined} onValueChange={onCommit} disabled={disabled}>
      <SelectTrigger className={className}>
        <SelectValue placeholder={placeholder} />
      </SelectTrigger>
      <SelectContent>
        {options.map((o) => (
          <SelectItem key={o.value} value={o.value}>
            {o.label}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}
