import { executeOnStream, createStream, flushStream, nextId, peekQuery } from '../../../wasm.ts';
import { duplicateInvoice as duplicateInvoiceCmd } from '../../../generated/InvoiceCommandFactories.ts';
import { isoDate } from './isoDate.ts';

/**
 * Duplicate an invoice: copy header + all positions into a new draft.
 * Returns the new id, or null if the source doesn't exist.
 *
 * All UUIDs are pre-computed here so that the optimistic (client) and
 * server-confirmed inserts share the same primary keys (idempotent re-apply).
 */
export async function duplicateInvoice(sourceInvoiceId: string): Promise<string | null> {
  const headerRows = peekQuery(
    `SELECT invoices.number FROM invoices WHERE invoices.id = UUID '${sourceInvoiceId}'`,
  );
  if (headerRows.length === 0) return null;
  const srcNumber = headerRows[0][0] as string;

  const positionRows = peekQuery(
    `SELECT positions.id FROM positions WHERE positions.invoice_id = UUID '${sourceInvoiceId}' ORDER BY positions.position_nr`,
  );
  const newPositionIds: string[] = positionRows.map(() => nextId());
  const newInvoiceId = nextId();

  const stream = createStream(1);
  executeOnStream(stream, duplicateInvoiceCmd({
    source_invoice_id: sourceInvoiceId,
    new_invoice_id: newInvoiceId,
    new_position_ids: newPositionIds,
    new_number: `${srcNumber}-KOPIE`,
    date_issued: isoDate(0),
    date_due: isoDate(14),
  }));
  await flushStream(stream).catch(() => {});
  return newInvoiceId;
}
