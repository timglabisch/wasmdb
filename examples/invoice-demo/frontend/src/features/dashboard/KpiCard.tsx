import * as React from 'react';
import type { LucideIcon } from 'lucide-react';
import { Card } from '@/components/ui/card';
import { cn } from '@/lib/cn';

export type KpiTone = 'default' | 'warning' | 'destructive' | 'success' | 'muted';

export interface KpiCardProps {
  label: string;
  value: React.ReactNode;
  hint?: React.ReactNode;
  icon: LucideIcon;
  tone?: KpiTone;
}

const TONE_VALUE: Record<KpiTone, string> = {
  default: 'text-foreground',
  warning: 'text-warning',
  destructive: 'text-destructive',
  success: 'text-success',
  muted: 'text-muted-foreground',
};

/**
 * Single KPI tile. Kept memo-friendly: only rerenders when its props change,
 * so each tile rerenders independently from its neighbours when the
 * parent-level subscription it sources from emits.
 */
export const KpiCard = React.memo(function KpiCard({
  label,
  value,
  hint,
  icon: Icon,
  tone = 'default',
}: KpiCardProps) {
  return (
    <Card className="relative flex flex-col gap-2 border-border p-4 shadow-none">
      <div className="flex items-start justify-between gap-2">
        <span className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
          {label}
        </span>
        <Icon className="h-3.5 w-3.5 shrink-0 text-muted-foreground" aria-hidden />
      </div>
      <div className={cn('text-2xl font-semibold leading-tight tabular-nums', TONE_VALUE[tone])}>
        {value}
      </div>
      {hint != null && (
        <div className="text-xs text-muted-foreground">{hint}</div>
      )}
    </Card>
  );
});
