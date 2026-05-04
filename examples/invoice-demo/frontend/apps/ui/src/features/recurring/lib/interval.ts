/**
 * Format an interval (unit + value) into human-readable German.
 * Handles the common cases with nice shortcuts.
 */
export function formatInterval(unit: string, value: number): string {
  const v = Math.max(1, value | 0);
  if (v === 1) {
    switch (unit) {
      case 'day':   return 'Täglich';
      case 'week':  return 'Wöchentlich';
      case 'month': return 'Monatlich';
      case 'year':  return 'Jährlich';
    }
  }
  const unitLabel: Record<string, string> = {
    day: 'Tage',
    week: 'Wochen',
    month: 'Monate',
    year: 'Jahre',
  };
  const label = unitLabel[unit] ?? unit;
  return `Alle ${v} ${label}`;
}

/** Short relative-days hint like "heute", "morgen", "in 3 Tagen", "vor 2 Tagen". */
export function formatRelativeDays(days: number): string {
  if (days === 0) return 'heute';
  if (days === 1) return 'morgen';
  if (days === -1) return 'gestern';
  if (days > 0) return `in ${days} Tagen`;
  return `vor ${Math.abs(days)} Tagen`;
}

export const INTERVAL_UNIT_OPTIONS = [
  { value: 'day',   label: 'Tag' },
  { value: 'week',  label: 'Woche' },
  { value: 'month', label: 'Monat' },
  { value: 'year',  label: 'Jahr' },
];

export const STATUS_TEMPLATE_OPTIONS = [
  { value: 'draft',     label: 'Entwurf' },
  { value: 'sent',      label: 'Versendet' },
  { value: 'paid',      label: 'Bezahlt' },
  { value: 'cancelled', label: 'Storniert' },
];

/** Advance an ISO date by (value, unit). Used to derive the next next_run on execution. */
export function advanceDate(iso: string, unit: string, value: number): string {
  const base = iso && !Number.isNaN(Date.parse(iso)) ? new Date(iso) : new Date();
  const v = Math.max(1, value | 0);
  switch (unit) {
    case 'day':   base.setDate(base.getDate() + v); break;
    case 'week':  base.setDate(base.getDate() + v * 7); break;
    case 'month': base.setMonth(base.getMonth() + v); break;
    case 'year':  base.setFullYear(base.getFullYear() + v); break;
  }
  return base.toISOString().slice(0, 10);
}
