import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'DeleteRecurringPosition' }>;

const DEFAULTS: Omit<Variant, 'type'> = { id: 0 };

export function deleteRecurringPosition(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'DeleteRecurringPosition', ...DEFAULTS, ...args };
}
