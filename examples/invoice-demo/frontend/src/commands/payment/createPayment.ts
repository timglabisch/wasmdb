import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'CreatePayment' }>;

const DEFAULTS: Omit<Variant, 'type'> = {
  id: 0, invoice_id: 0, amount: 0, paid_at: '',
  method: 'transfer', reference: '', note: '',
};

export function createPayment(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'CreatePayment', ...DEFAULTS, ...args };
}
