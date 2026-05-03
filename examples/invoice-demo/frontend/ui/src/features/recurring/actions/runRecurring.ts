import {
  executeOnStream, createStream, flushStream, nextId, peekQuery,
} from '@/wasm';
import { runRecurringOnce } from '@/generated/InvoiceCommandFactories';
import { advanceDate } from '../lib/interval';

const DOC_PREFIX = 'INV';

interface RunRecurringResult {
  invoiceId: string;
  invoiceNumber: string;
}

function isoDate(offsetDays = 0): string {
  const d = new Date();
  d.setDate(d.getDate() + offsetDays);
  return d.toISOString().slice(0, 10);
}

function peekTemplateHeader(recurringId: string) {
  const rows = peekQuery(
    `SELECT recurring_invoices.template_name, recurring_invoices.customer_id, ` +
    `recurring_invoices.interval_unit, recurring_invoices.interval_value, recurring_invoices.next_run ` +
    `FROM recurring_invoices WHERE recurring_invoices.id = UUID '${recurringId}'`,
  );
  if (rows.length === 0) return null;
  const r = rows[0];
  return {
    template_name: r[0] as string,
    customer_id: r[1] as string,
    interval_unit: r[2] as string,
    interval_value: r[3] as number,
    next_run: r[4] as string,
  };
}

function peekPositionCount(recurringId: string): number {
  const rows = peekQuery(
    `SELECT recurring_positions.id FROM recurring_positions ` +
    `WHERE recurring_positions.recurring_id = UUID '${recurringId}'`,
  );
  return rows.length;
}

function peekPaymentTermsDays(customerId: string): number {
  if (!customerId) return 14;
  const rows = peekQuery(
    `SELECT customers.payment_terms_days FROM customers WHERE customers.id = UUID '${customerId}'`,
  );
  if (rows.length === 0) return 14;
  const v = rows[0][0] as number;
  return v || 14;
}

/**
 * Materialize a recurring template into a concrete invoice + positions in one
 * atomic stream, with an audit entry produced by the command itself.
 * Returns the created invoice id/number.
 */
export async function runRecurringAction(recurringId: string): Promise<RunRecurringResult | null> {
  const header = peekTemplateHeader(recurringId);
  if (!header) return null;

  const newInvoiceId = nextId();
  const dueDays = peekPaymentTermsDays(header.customer_id);
  const newNumber = `${DOC_PREFIX}-${new Date().getFullYear()}-${newInvoiceId.slice(0, 8)}`;
  const issueDate = header.next_run && header.next_run <= isoDate(7) ? header.next_run : isoDate();
  const dueDate = isoDate(dueDays);
  const newNextRun = advanceDate(header.next_run || issueDate, header.interval_unit, header.interval_value);

  // Allocate an id per position up-front so the command is fully self-contained.
  const positionCount = peekPositionCount(recurringId);
  const positionIds: string[] = [];
  for (let i = 0; i < positionCount; i++) positionIds.push(nextId());

  const stream = createStream(1);
  executeOnStream(stream, runRecurringOnce({
    recurring_id: recurringId,
    new_invoice_id: newInvoiceId,
    position_ids: positionIds,
    new_number: newNumber,
    issue_date: issueDate,
    due_date: dueDate,
    new_next_run: newNextRun,
  }));
  await flushStream(stream).catch(() => {});
  return { invoiceId: newInvoiceId, invoiceNumber: newNumber };
}
