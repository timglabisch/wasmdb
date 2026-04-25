import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'DeleteCustomer' }>;

const DEFAULTS: Omit<Variant, 'type'> = { id: '' };

export function deleteCustomer(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'DeleteCustomer', ...DEFAULTS, ...args };
}
