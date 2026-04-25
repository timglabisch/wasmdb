import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'DeleteContact' }>;

const DEFAULTS: Omit<Variant, 'type'> = { id: '' };

export function deleteContact(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'DeleteContact', ...DEFAULTS, ...args };
}
