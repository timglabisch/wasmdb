import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'AddRecurringPosition' }>;

const DEFAULTS: Omit<Variant, 'type'> = {
  id: '', recurring_id: '', position_nr: 0,
  description: '', quantity: 1000, unit_price: 0, tax_rate: 1900,
  unit: 'Stk', item_number: '', discount_pct: 0,
};

export function addRecurringPosition(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'AddRecurringPosition', ...DEFAULTS, ...args };
}
