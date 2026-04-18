import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'RunRecurringOnce' }>;

const DEFAULTS: Omit<Variant, 'type'> = {
  recurring_id: 0, new_invoice_id: 0,
  position_ids: [], new_number: '',
  issue_date: '', due_date: '', new_next_run: '',
};

/** Materializes a recurring template into a concrete invoice + positions, in one atomic step. */
export function runRecurringOnce(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'RunRecurringOnce', ...DEFAULTS, ...args };
}
