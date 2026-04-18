import {
  executeOnStream, createStream, flushStream, nextId, peekQuery,
} from '@/wasm';
import { selectById, selectByFk } from '@/queries';
import { runRecurringOnce } from '@/commands/recurring/runRecurringOnce';
import { logActivity } from '@/commands/activity/logActivity';
import { advanceDate } from '../lib/interval';

const DOC_PREFIX = 'INV';

interface RunRecurringResult {
  invoiceId: number;
  invoiceNumber: string;
}

function isoDate(offsetDays = 0): string {
  const d = new Date();
  d.setDate(d.getDate() + offsetDays);
  return d.toISOString().slice(0, 10);
}

function peekTemplateHeader(recurringId: number) {
  const rows = peekQuery(selectById(
    'recurring_invoices',
    'template_name, customer_id, interval_unit, interval_value, next_run',
    recurringId,
  ));
  if (rows.length === 0) return null;
  const r = rows[0];
  return {
    template_name: r[0] as string,
    customer_id: r[1] as number,
    interval_unit: r[2] as string,
    interval_value: r[3] as number,
    next_run: r[4] as string,
  };
}

function peekPositionCount(recurringId: number): number {
  const rows = peekQuery(selectByFk(
    'recurring_positions', 'id', 'recurring_id', recurringId,
  ));
  return rows.length;
}

function peekPaymentTermsDays(customerId: number): number {
  if (customerId <= 0) return 14;
  const rows = peekQuery(selectById('customers', 'payment_terms_days', customerId));
  if (rows.length === 0) return 14;
  const v = rows[0][0] as number;
  return v || 14;
}

/**
 * Materialize a recurring template into a concrete invoice + positions in one
 * atomic stream, plus an audit entry. Returns the created invoice id/number.
 */
export async function runRecurringAction(recurringId: number): Promise<RunRecurringResult | null> {
  const header = peekTemplateHeader(recurringId);
  if (!header) return null;

  const newInvoiceId = nextId();
  const dueDays = peekPaymentTermsDays(header.customer_id);
  const newNumber = `${DOC_PREFIX}-${new Date().getFullYear()}-${String(newInvoiceId).padStart(4, '0')}`;
  const issueDate = header.next_run && header.next_run <= isoDate(7) ? header.next_run : isoDate();
  const dueDate = isoDate(dueDays);
  const newNextRun = advanceDate(header.next_run || issueDate, header.interval_unit, header.interval_value);

  // Allocate an id per position up-front so the command is fully self-contained.
  const positionCount = peekPositionCount(recurringId);
  const positionIds: number[] = [];
  for (let i = 0; i < positionCount; i++) positionIds.push(nextId());

  const stream = createStream(2);
  executeOnStream(stream, runRecurringOnce({
    recurring_id: recurringId,
    new_invoice_id: newInvoiceId,
    position_ids: positionIds,
    new_number: newNumber,
    issue_date: issueDate,
    due_date: dueDate,
    new_next_run: newNextRun,
  }));
  executeOnStream(stream, logActivity({
    entityType: 'recurring', entityId: recurringId,
    action: 'run',
    detail: `Serie "${header.template_name}" ausgeführt — Rechnung ${newNumber} erstellt`,
  }));
  await flushStream(stream);
  return { invoiceId: newInvoiceId, invoiceNumber: newNumber };
}
