import { peekQuery } from '@/wasm';
import { selectById } from '@/queries';
import type { RecurringRow } from '../types';

const COLS =
  'customer_id, template_name, interval_unit, interval_value, ' +
  'next_run, last_run, enabled, status_template, notes_template';

const rowToRecurring = (r: any[]): RecurringRow => ({
  customer_id: r[0] as number,
  template_name: r[1] as string,
  interval_unit: r[2] as string,
  interval_value: r[3] as number,
  next_run: r[4] as string,
  last_run: r[5] as string,
  enabled: r[6] as number,
  status_template: r[7] as string,
  notes_template: r[8] as string,
});

/** One-shot non-reactive full-row read. Used at write time to compose UpdateRecurring payloads. */
export function peekRecurring(recurringId: number): RecurringRow | null {
  const rows = peekQuery(selectById('recurring_invoices', COLS, recurringId));
  if (rows.length === 0) return null;
  return rowToRecurring(rows[0]);
}
