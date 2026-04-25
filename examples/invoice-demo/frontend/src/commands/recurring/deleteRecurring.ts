import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'DeleteRecurring' }>;

const DEFAULTS: Omit<Variant, 'type'> = { id: '' };

export function deleteRecurring(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'DeleteRecurring', ...DEFAULTS, ...args };
}
