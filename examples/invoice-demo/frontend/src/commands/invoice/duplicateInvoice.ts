import type { InvoiceCommand } from '../../generated/InvoiceCommand.ts';
import { nextId, peekQuery } from '../../wasm.ts';
import { isoDate } from '../../features/invoice/actions/isoDate.ts';

type Variant = Extract<InvoiceCommand, { type: 'DuplicateInvoice' }>;

/**
 * Build a DuplicateInvoice intent command.
 *
 * All UUIDs are pre-computed here so that the optimistic (client) and
 * server-confirmed inserts share the same primary keys (idempotent re-apply).
 * Returns null when the source invoice is not found in the local DB.
 */
export function duplicateInvoice(sourceInvoiceId: string): { cmd: InvoiceCommand; newInvoiceId: string } | null {
  // Peek source invoice number for the new number string.
  const headerRows = peekQuery(
    `SELECT invoices.number FROM invoices WHERE invoices.id = UUID '${sourceInvoiceId}'`,
  );
  if (headerRows.length === 0) return null;
  const srcNumber = headerRows[0][0] as string;
  const newNumber = `${srcNumber}-KOPIE`;

  // Count source positions to allocate the right number of new UUIDs.
  const positionRows = peekQuery(
    `SELECT positions.id FROM positions WHERE positions.invoice_id = UUID '${sourceInvoiceId}' ORDER BY positions.position_nr`,
  );
  const newPositionIds: string[] = positionRows.map(() => nextId());

  const newInvoiceId = nextId();

  const cmd: Variant = {
    type: 'DuplicateInvoice',
    source_invoice_id: sourceInvoiceId,
    new_invoice_id: newInvoiceId,
    new_position_ids: newPositionIds,
    new_number: newNumber,
    date_issued: isoDate(0),
    date_due: isoDate(14),
    activity_id: nextId(),
    timestamp: new Date().toISOString(),
  };
  return { cmd, newInvoiceId };
}
