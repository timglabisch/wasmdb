import * as React from 'react';
import { Label } from '@/components/ui/label';
import { cn } from '@/lib/cn';

/**
 * Uniform label + control wrapper used on every edit surface.
 * Horizontal by default (label left, control right) to keep rows dense.
 */
export function Field({
  label,
  htmlFor,
  hint,
  children,
  className,
}: {
  label?: React.ReactNode;
  htmlFor?: string;
  hint?: React.ReactNode;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <div className={cn('grid grid-cols-[140px_1fr] items-center gap-2 py-0.5', className)}>
      {label !== undefined ? (
        <Label htmlFor={htmlFor} className="justify-self-start text-xs text-muted-foreground font-normal">
          {label}
        </Label>
      ) : (
        <span />
      )}
      <div className="min-w-0">
        {children}
        {hint && <div className="mt-0.5 text-[11px] text-muted-foreground">{hint}</div>}
      </div>
    </div>
  );
}

/**
 * A section within a form card: title + optional description + children stack.
 */
export function FormSection({
  title,
  description,
  children,
  className,
}: {
  title?: React.ReactNode;
  description?: React.ReactNode;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <section className={cn('space-y-0.5 py-3 first:pt-0 last:pb-0', className)}>
      {(title || description) && (
        <header className="mb-1.5">
          {title && <h3 className="text-[11px] font-medium uppercase tracking-wider text-muted-foreground">{title}</h3>}
          {description && <p className="mt-0.5 text-xs text-muted-foreground">{description}</p>}
        </header>
      )}
      <div className="space-y-0.5">{children}</div>
    </section>
  );
}
