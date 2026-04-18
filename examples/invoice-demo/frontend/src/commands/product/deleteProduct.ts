import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'DeleteProduct' }>;

const DEFAULTS: Omit<Variant, 'type'> = { id: 0 };

export function deleteProduct(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'DeleteProduct', ...DEFAULTS, ...args };
}
