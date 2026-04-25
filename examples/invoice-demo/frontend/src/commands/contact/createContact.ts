import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'CreateContact' }>;

const DEFAULTS: Omit<Variant, 'type'> = {
  id: '', customer_id: '', name: '', email: '', phone: '', role: '',
  is_primary: 0,
};

export function createContact(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'CreateContact', ...DEFAULTS, ...args };
}
