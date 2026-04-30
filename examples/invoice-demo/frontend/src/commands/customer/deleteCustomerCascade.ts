import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';
import { nextId } from '../../wasm.ts';

type Variant = Extract<InvoiceCommand, { type: 'DeleteCustomerCascade' }>;

/**
 * Full tree delete: customer + contacts + invoices + positions + payments +
 * sepa_mandates + recurring templates + recurring positions, all atomic.
 * The activity_log row is emitted by the command itself; callers do not
 * need a separate logActivity call.
 */
export function deleteCustomerCascade(args: { id: string; name: string }): InvoiceCommand {
  const cmd: Variant = {
    type: 'DeleteCustomerCascade',
    id: args.id,
    name: args.name,
    activity_id: nextId(),
    timestamp: new Date().toISOString(),
  };
  return cmd;
}
