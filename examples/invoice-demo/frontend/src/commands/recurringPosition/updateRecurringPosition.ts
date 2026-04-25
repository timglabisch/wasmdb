import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'UpdateRecurringPosition' }>;

const DEFAULTS: Omit<Variant, 'type'> = {
  id: '',
  description: '', quantity: 0, unit_price: 0, tax_rate: 1900,
  unit: 'Stk', item_number: '', discount_pct: 0,
};

export function updateRecurringPosition(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'UpdateRecurringPosition', ...DEFAULTS, ...args };
}
