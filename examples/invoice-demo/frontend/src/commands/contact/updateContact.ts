import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'UpdateContact' }>;

const DEFAULTS: Omit<Variant, 'type'> = {
  id: '', name: '', email: '', phone: '', role: '', is_primary: 0,
};

export function updateContact(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'UpdateContact', ...DEFAULTS, ...args };
}
