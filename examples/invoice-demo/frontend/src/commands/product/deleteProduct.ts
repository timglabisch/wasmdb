import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'DeleteProduct' }>;

const DEFAULTS: Omit<Variant, 'type'> = { id: '' };

export function deleteProduct(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'DeleteProduct', ...DEFAULTS, ...args };
}
