import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'DeletePayment' }>;

const DEFAULTS: Omit<Variant, 'type'> = { id: '' };

export function deletePayment(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'DeletePayment', ...DEFAULTS, ...args };
}
