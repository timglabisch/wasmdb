import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';
import { nextId } from '../../wasm.ts';

type Variant = Extract<InvoiceCommand, { type: 'DeleteProduct' }>;

/** Delete a product. The activity_log row is emitted by the command itself;
 * callers do not need a separate logActivity call. */
export function deleteProduct(args: { id: string; name: string }): InvoiceCommand {
  const cmd: Variant = {
    type: 'DeleteProduct',
    id: args.id,
    name: args.name,
    activity_id: nextId(),
    timestamp: new Date().toISOString(),
  };
  return cmd;
}
