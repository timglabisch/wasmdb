import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';

type Variant = Extract<InvoiceCommand, { type: 'MovePosition' }>;

const DEFAULTS: Omit<Variant, 'type'> = { id: '', new_position_nr: 0 };

export function movePosition(args: Partial<Omit<Variant, 'type'>> = {}): InvoiceCommand {
  return { type: 'MovePosition', ...DEFAULTS, ...args };
}
