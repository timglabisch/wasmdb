import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'CreateProduct' }>;

const DEFAULTS: Omit<Variant, 'type'> = {
  id: 0, sku: '', name: '', description: '',
  unit: 'Stk', unit_price: 0, tax_rate: 1900, cost_price: 0,
  active: 1,
};

export function createProduct(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'CreateProduct', ...DEFAULTS, ...args };
}
