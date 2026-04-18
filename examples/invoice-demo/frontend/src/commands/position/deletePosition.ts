import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'DeletePosition' }>;

const DEFAULTS: Omit<Variant, 'type'> = { id: 0 };

export function deletePosition(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'DeletePosition', ...DEFAULTS, ...args };
}
