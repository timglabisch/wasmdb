import { memo, useCallback } from 'react';
import { useNavigate } from '@tanstack/react-router';
import { MoreHorizontal, Copy, Trash2 } from 'lucide-react';
import { toast } from '@/components/ui/sonner';
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { Button } from '@/components/ui/button';
import { TableCell, TableRow } from '@/components/ui/table';
import { Badge } from '@/components/ui/badge';
import {
  DocTypeBadge, InvoiceStatusBadge, PaymentStatusBadge,
  computePaymentStatus, isOverdue,
} from '@/shared/lib/status';
import { formatDateISO, formatEuro } from '@/shared/lib/format';
import { useQuery } from '@/wasm';
import { selectById, selectByFk } from '@/queries';
import { computeGrossCents } from '@/shared/lib/gross';
import { duplicateInvoice } from '@/features/invoice/actions/duplicateInvoice';
import { deleteInvoiceWithConfirm } from '@/features/invoice/actions/deleteInvoiceWithConfirm';

interface Header {
  number: string;
  doc_type: string;
  status: string;
  date_issued: string;
  date_due: string;
  customer_id: number;
}

interface GrossPos {
  quantity: number;
  unit_price: number;
  tax_rate: number;
  discount_pct: number;
  position_type: string;
}

/**
 * Subscribes only to the invoice id it owns. Each cell that needs sub-data is its
 * own memoized child with the smallest possible per-column subscription.
 */
export const InvoiceListRow = memo(function InvoiceListRow({
  invoiceId,
  searchTerm,
}: {
  invoiceId: number;
  searchTerm: string;
}) {
  const navigate = useNavigate();

  // Header columns used by this row.
  const headers = useQuery<Header>(
    selectById(
      'invoices',
      'number, doc_type, status, date_issued, date_due, customer_id',
      invoiceId,
    ),
    ([number, doc_type, status, date_issued, date_due, customer_id]) => ({
      number: number as string,
      doc_type: doc_type as string,
      status: status as string,
      date_issued: date_issued as string,
      date_due: date_due as string,
      customer_id: customer_id as number,
    }),
  );

  const h = headers[0];

  // Client-side filter on invoice number. Returning null avoids rendering the row.
  const onOpen = useCallback(() => {
    navigate({ to: '/invoices/$invoiceId', params: { invoiceId } });
  }, [invoiceId, navigate]);

  const onDuplicate = useCallback(async () => {
    const newId = await duplicateInvoice(invoiceId);
    if (newId !== null) {
      toast.success('Rechnung dupliziert');
    }
  }, [invoiceId]);

  const onDelete = useCallback(async () => {
    if (!h) return;
    const ok = await deleteInvoiceWithConfirm(invoiceId, h.number);
    if (ok) toast.success('Rechnung gelöscht');
  }, [invoiceId, h]);

  if (!h) return null;
  if (searchTerm) {
    const t = searchTerm.toLowerCase();
    if (!h.number.toLowerCase().includes(t)) return null;
  }

  const overdue = isOverdue(h.date_due, h.status);

  return (
    <TableRow
      onClick={onOpen}
      className="cursor-pointer"
    >
      <TableCell className="font-medium">{h.number || `#${invoiceId}`}</TableCell>
      <TableCell><DocTypeBadge docType={h.doc_type} /></TableCell>
      <TableCell><CustomerCell customerId={h.customer_id} /></TableCell>
      <TableCell className="whitespace-nowrap text-muted-foreground">
        {formatDateISO(h.date_issued)}
      </TableCell>
      <TableCell className="whitespace-nowrap">
        <span className={overdue ? 'text-destructive' : 'text-muted-foreground'}>
          {formatDateISO(h.date_due)}
        </span>
        {overdue && (
          <Badge variant="destructive" className="ml-2">überfällig</Badge>
        )}
      </TableCell>
      <TableCell className="text-right tabular-nums">
        <GrossCell invoiceId={invoiceId} />
      </TableCell>
      <TableCell><InvoiceStatusBadge status={h.status} /></TableCell>
      <TableCell><PaymentCell invoiceId={invoiceId} /></TableCell>
      <TableCell className="w-[1%]" onClick={(e) => e.stopPropagation()}>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button variant="ghost" size="icon" className="h-8 w-8">
              <MoreHorizontal className="h-4 w-4" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            <DropdownMenuItem onSelect={onDuplicate}>
              <Copy className="h-4 w-4" /> Duplizieren
            </DropdownMenuItem>
            <DropdownMenuItem onSelect={onDelete} className="text-destructive focus:text-destructive">
              <Trash2 className="h-4 w-4" /> Löschen
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </TableCell>
    </TableRow>
  );
});

/** Subscribes to customers.name for the given id. */
const CustomerCell = memo(function CustomerCell({ customerId }: { customerId: number }) {
  const rows = useQuery<string>(
    selectById('customers', 'name', customerId),
    ([name]) => name as string,
  );
  return <span className="text-sm">{rows[0] ?? '—'}</span>;
});

/** Subscribes to all positions of this invoice and shows gross. */
const GrossCell = memo(function GrossCell({ invoiceId }: { invoiceId: number }) {
  const positions = useQuery<GrossPos>(
    selectByFk(
      'positions',
      'quantity, unit_price, tax_rate, discount_pct, position_type',
      'invoice_id', invoiceId, 'position_nr',
    ),
    ([q, p, t, d, pt]) => ({
      quantity: q as number,
      unit_price: p as number,
      tax_rate: t as number,
      discount_pct: d as number,
      position_type: pt as string,
    }),
  );
  return <span>{formatEuro(computeGrossCents(positions))}</span>;
});

/** Subscribes to payments + positions to compute payment status. */
const PaymentCell = memo(function PaymentCell({ invoiceId }: { invoiceId: number }) {
  const positions = useQuery<GrossPos>(
    selectByFk(
      'positions',
      'quantity, unit_price, tax_rate, discount_pct, position_type',
      'invoice_id', invoiceId, 'position_nr',
    ),
    ([q, p, t, d, pt]) => ({
      quantity: q as number,
      unit_price: p as number,
      tax_rate: t as number,
      discount_pct: d as number,
      position_type: pt as string,
    }),
  );
  const payments = useQuery<number>(
    selectByFk('payments', 'amount', 'invoice_id', invoiceId),
    ([a]) => a as number,
  );
  const gross = computeGrossCents(positions);
  const paid = payments.reduce((acc, a) => acc + a, 0);
  return <PaymentStatusBadge status={computePaymentStatus(gross, paid)} />;
});
