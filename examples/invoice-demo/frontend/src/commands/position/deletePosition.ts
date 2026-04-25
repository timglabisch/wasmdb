import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'DeletePosition' }>;

const DEFAULTS: Omit<Variant, 'type'> = { id: '' };

export function deletePosition(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'DeletePosition', ...DEFAULTS, ...args };
}
