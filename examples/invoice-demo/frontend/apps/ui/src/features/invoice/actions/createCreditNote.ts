import { createStream, flushStream, nextId, peekQuery } from '@wasmdb/client';
import { executeOnStream } from '@/commands';
import { createCreditNote as createCreditNoteCmd } from 'invoice-demo-generated/InvoiceCommandFactories';
import { isoDate } from './isoDate.ts';

/**
 * Create a credit note for an existing invoice: fresh draft with negated
 * quantities + parent_id link back to the source invoice.
 *
 * All UUIDs are pre-computed here so that the optimistic (client) and
 * server-confirmed inserts share the same primary keys (idempotent re-apply).
 */
export async function createCreditNote(sourceInvoiceId: string): Promise<string | null> {
  const headerRows = peekQuery(
    `SELECT invoices.number FROM invoices WHERE invoices.id = :id`,
    { id: sourceInvoiceId },
  );
  if (headerRows.length === 0) return null;
  const srcNumber = headerRows[0][0] as string;
  if (!confirm(`Gutschrift zu "${srcNumber}" erzeugen?`)) return null;

  const positionRows = peekQuery(
    `SELECT positions.id FROM positions WHERE positions.invoice_id = :id ORDER BY positions.position_nr`,
    { id: sourceInvoiceId },
  );
  const newPositionIds: string[] = positionRows.map(() => nextId());
  const newInvoiceId = nextId();

  const stream = createStream(1);
  executeOnStream(stream, createCreditNoteCmd({
    source_invoice_id: sourceInvoiceId,
    new_invoice_id: newInvoiceId,
    new_position_ids: newPositionIds,
    new_number: `CN-${srcNumber}`,
    date_issued: isoDate(0),
    date_due: isoDate(14),
  }));
  await flushStream(stream).catch(() => {});
  return newInvoiceId;
}
