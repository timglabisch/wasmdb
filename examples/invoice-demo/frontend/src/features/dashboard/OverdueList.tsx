import * as React from 'react';
import { Link } from '@tanstack/react-router';
import { CheckCircle2 } from 'lucide-react';
import { useQuery } from '@/wasm';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { formatEuro, relativeDaysFromToday } from '@/shared/lib/format';
import { useInvoiceGrossCents } from '@/shared/lib/gross';
import { cn } from '@/lib/cn';

/**
 * Reactive list of overdue invoices.
 * Each row is its own memoized subscription so one invoice mutation does not
 * rerender siblings. The parent subscribes to only the id list + date_due.
 */
export function OverdueList() {
  const today = React.useMemo(() => new Date().toISOString().slice(0, 10), []);

  // Parent subscription: minimal — just the ids of overdue invoices.
  // We still pull date_due so sorting is stable when mutations land.
  const ids = useQuery(
    `SELECT invoices.id, invoices.date_due FROM invoices ` +
      `WHERE invoices.doc_type = 'invoice' ` +
      `AND invoices.status IN ('draft', 'sent') ` +
      `AND invoices.date_due < '${today}' ` +
      `ORDER BY invoices.date_due ASC`,
    ([id, dueDate]) => ({ id: id as string, dueDate: dueDate as string }),
  );

  return (
    <Card className="flex flex-col border-border shadow-none">
      <CardHeader className="flex flex-row items-center justify-between space-y-0 p-4">
        <CardTitle className="text-sm font-semibold">Überfällige Rechnungen</CardTitle>
        <Badge variant={ids.length > 0 ? 'destructive' : 'muted'}>{ids.length}</Badge>
      </CardHeader>
      <CardContent className="p-2 pt-0">
        {ids.length === 0 ? (
          <EmptyState />
        ) : (
          <ul className="flex flex-col">
            {ids.map((row) => (
              <OverdueRow key={row.id} invoiceId={row.id} />
            ))}
          </ul>
        )}
      </CardContent>
    </Card>
  );
}

function EmptyState() {
  return (
    <div className="flex flex-col items-center justify-center gap-2 p-8 text-center">
      <CheckCircle2 className="h-6 w-6 text-muted-foreground" aria-hidden />
      <div className="text-sm text-muted-foreground">Keine überfälligen Rechnungen</div>
    </div>
  );
}

interface OverdueRowProps {
  invoiceId: string;
}

/**
 * Per-row subscription: the row only reads the columns it actually renders.
 * A payment added to invoice A will not rerender row B.
 */
const OverdueRow = React.memo(function OverdueRow({ invoiceId }: OverdueRowProps) {
  const invoice = useQuery(
    `SELECT invoices.number, invoices.date_due, invoices.customer_id, invoices.status, invoices.doc_type ` +
    `FROM invoices WHERE invoices.id = UUID '${invoiceId}'`,
    ([number, dateDue, customerId, status, docType]) => ({
      number: number as string,
      dateDue: dateDue as string,
      customerId: (customerId as string | null) ?? null,
      status: status as string,
      docType: docType as string,
    }),
  )[0];

  if (!invoice) return null;

  const daysOverdue = Math.max(0, -relativeDaysFromToday(invoice.dateDue));

  return (
    <li>
      <Link
        to="/invoices/$invoiceId"
        params={{ invoiceId }}
        className={cn(
          'flex h-12 items-center gap-3 rounded-md px-3 transition-colors',
          'hover:bg-muted/50',
        )}
      >
        <div className="flex min-w-0 flex-1 items-center gap-3">
          <span className="shrink-0 font-mono text-sm font-semibold tabular-nums">
            {invoice.number || '—'}
          </span>
          <CustomerName customerId={invoice.customerId} />
        </div>
        <span className="shrink-0 text-xs font-medium text-destructive">
          {daysOverdue === 1 ? '1 Tag überfällig' : `${daysOverdue} Tage überfällig`}
        </span>
        <OpenAmount invoiceId={invoiceId} />
      </Link>
    </li>
  );
});

const CustomerName = React.memo(function CustomerName({ customerId }: { customerId: string | null }) {
  const lookupId = customerId ?? '00000000-0000-0000-0000-000000000000';
  const rows = useQuery(
    `SELECT customers.name FROM customers WHERE customers.id = UUID '${lookupId}'`,
    ([name]) => name as string,
  );
  const name = rows[0];
  if (!customerId) {
    return <span className="min-w-0 truncate text-sm text-muted-foreground">—</span>;
  }
  if (name === undefined) {
    return <Skeleton className="h-3.5 w-32" />;
  }
  return (
    <span className="min-w-0 truncate text-sm text-muted-foreground">
      {name || '—'}
    </span>
  );
});

const OpenAmount = React.memo(function OpenAmount({ invoiceId }: { invoiceId: string }) {
  const gross = useInvoiceGrossCents(invoiceId);
  const payments = useQuery(
    `SELECT payments.amount FROM payments WHERE payments.invoice_id = UUID '${invoiceId}'`,
    ([amount]) => amount as number,
  );
  const paid = payments.reduce((s, n) => s + n, 0);
  const open = gross - paid;
  return (
    <span className="shrink-0 text-sm font-semibold tabular-nums text-destructive">
      {formatEuro(open)}
    </span>
  );
});
