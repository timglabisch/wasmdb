import * as React from 'react';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { useSyncedDraft } from './useSyncedDraft';
import { cn } from '@/lib/cn';

/**
 * Text input that commits on blur or Enter. Escape reverts to source.
 * Matches the reactive-db demo pattern: value comes from the live query,
 * edits are local drafts committed as UpdateX commands on blur.
 */
export function BlurInput({
  value,
  onCommit,
  className,
  onKeyDown: onKeyDownExternal,
  ...rest
}: {
  value: string;
  onCommit: (next: string) => void;
} & Omit<React.InputHTMLAttributes<HTMLInputElement>, 'value' | 'onChange'>) {
  const [draft, setDraft, dirty, reset] = useSyncedDraft(value);
  const commit = () => {
    if (dirty && draft !== value) onCommit(draft);
  };
  return (
    <Input
      {...rest}
      className={className}
      value={draft}
      onChange={(e) => setDraft(e.target.value)}
      onBlur={commit}
      onKeyDown={(e) => {
        onKeyDownExternal?.(e);
        if (e.defaultPrevented) return;
        if (e.key === 'Enter') (e.target as HTMLInputElement).blur();
        if (e.key === 'Escape') reset();
      }}
    />
  );
}

export function BlurTextarea({
  value,
  onCommit,
  className,
  ...rest
}: {
  value: string;
  onCommit: (next: string) => void;
} & Omit<React.TextareaHTMLAttributes<HTMLTextAreaElement>, 'value' | 'onChange'>) {
  const [draft, setDraft, dirty, reset] = useSyncedDraft(value);
  const commit = () => {
    if (dirty && draft !== value) onCommit(draft);
  };
  return (
    <Textarea
      {...rest}
      className={cn('min-h-[80px]', className)}
      value={draft}
      onChange={(e) => setDraft(e.target.value)}
      onBlur={commit}
      onKeyDown={(e) => {
        if (e.key === 'Escape') reset();
      }}
    />
  );
}

export function BlurNumberInput({
  value,
  onCommit,
  min,
  step,
  className,
  ...rest
}: {
  value: number;
  onCommit: (next: number) => void;
} & Omit<React.InputHTMLAttributes<HTMLInputElement>, 'value' | 'onChange' | 'type'>) {
  const [draft, setDraft, dirty, reset] = useSyncedDraft<number | ''>(value);
  const commit = () => {
    const n = typeof draft === 'number' ? draft : Number(draft);
    if (!Number.isFinite(n)) return reset();
    const rounded = Math.round(n);
    if (dirty && rounded !== value) onCommit(rounded);
  };
  return (
    <Input
      {...rest}
      type="number"
      className={className}
      min={min}
      step={step}
      value={draft === '' ? '' : String(draft)}
      onChange={(e) => {
        const v = e.target.value;
        setDraft(v === '' ? '' : Number(v));
      }}
      onBlur={commit}
      onKeyDown={(e) => {
        if (e.key === 'Enter') (e.target as HTMLInputElement).blur();
        if (e.key === 'Escape') reset();
      }}
    />
  );
}

export function BlurDateInput({
  value,
  onCommit,
  className,
  ...rest
}: {
  value: string;
  onCommit: (next: string) => void;
} & Omit<React.InputHTMLAttributes<HTMLInputElement>, 'value' | 'onChange' | 'type'>) {
  const [draft, setDraft, dirty, reset] = useSyncedDraft(value);
  const commit = () => {
    if (dirty && draft !== value) onCommit(draft);
  };
  return (
    <Input
      {...rest}
      type="date"
      className={className}
      value={draft}
      onChange={(e) => setDraft(e.target.value)}
      onBlur={commit}
      onKeyDown={(e) => {
        if (e.key === 'Escape') reset();
      }}
    />
  );
}
