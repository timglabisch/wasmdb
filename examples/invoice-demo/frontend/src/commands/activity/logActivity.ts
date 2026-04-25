import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';
import { nextId } from '../../wasm.ts';

type Variant = Extract<InvoiceCommand, { type: 'LogActivity' }>;

export type EntityType = 'customer' | 'invoice' | 'payment' | 'recurring' | 'product' | 'sepa';

export interface ActivityInput {
  entityType: EntityType;
  entityId: string;
  action: string;
  detail: string;
}

/** Build a LogActivity command with auto-generated id + timestamp. */
export function logActivity(input: ActivityInput): InvoiceCommand {
  const cmd: Variant = {
    type: 'LogActivity',
    id: nextId(),
    timestamp: new Date().toISOString(),
    entity_type: input.entityType,
    entity_id: input.entityId,
    action: input.action,
    actor: 'demo',
    detail: input.detail,
  };
  return cmd;
}
