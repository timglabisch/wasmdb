import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';
import { nextId } from '../../wasm.ts';

type Variant = Extract<InvoiceCommand, { type: 'SetProductActive' }>;

/** Build a SetProductActive intent command. The activity row is emitted by the
 * command itself (see `commands/product/set_active.rs`) — caller does not
 * compose any LogActivity. */
export function setProductActive(args: { id: string; active: number }): InvoiceCommand {
  const cmd: Variant = {
    type: 'SetProductActive',
    id: args.id,
    active: args.active,
    activity_id: nextId(),
    timestamp: new Date().toISOString(),
  };
  return cmd;
}
