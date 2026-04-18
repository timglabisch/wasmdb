import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'UpdatePayment' }>;

const DEFAULTS: Omit<Variant, 'type'> = {
  id: 0, amount: 0, paid_at: '',
  method: 'transfer', reference: '', note: '',
};

export function updatePayment(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'UpdatePayment', ...DEFAULTS, ...args };
}
