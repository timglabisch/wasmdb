import * as React from 'react';
import { Link } from '@tanstack/react-router';
import { Users } from 'lucide-react';
import { useQuery } from '@/wasm';
import { Avatar, AvatarFallback } from '@/components/ui/avatar';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { formatEuro } from '@/shared/lib/format';
import { PaymentStatusBadge, computePaymentStatus } from '@/shared/lib/status';
import { cn } from '@/lib/cn';

const TOP_N = 10;

interface InvoicePosition {
  invoiceId: string;
  quantity: number;
  unitPrice: number;
  taxRate: number;
  discountPct: number;
  positionType: string;
}

interface CustomerTotals {
  customerId: string;
  gross: number;
  paid: number;
  invoiceCount: number;
}

/**
 * Reactive leaderboard of customers by gross revenue.
 *
 * Sorting requires aggregated totals at this level, so the parent subscribes
 * to the three streams needed to compute them. Row content (name, initials)
 * is still resolved inside a per-customer memoized wrapper so the avatar +
 * name label re-renders only when the customer row itself changes, not when
 * some unrelated payment lands.
 */
export function TopCustomers() {
  // Invoice → customer mapping. Only the two columns we need.
  const invoices = useQuery(
    `SELECT invoices.id, invoices.customer_id FROM invoices ` +
      `WHERE invoices.doc_type = 'invoice'`,
    ([id, customerId]) => ({ id: id as string, customerId: customerId as string }),
  );

  // All positions across all invoices. Gross is computed in JS because
  // the reactive planner rejects arithmetic inside SUM.
  const positions = useQuery(
    `SELECT positions.invoice_id, positions.quantity, positions.unit_price, ` +
      `positions.tax_rate, positions.discount_pct, positions.position_type FROM positions`,
    ([invoiceId, quantity, unitPrice, taxRate, discountPct, positionType]): InvoicePosition => ({
      invoiceId: invoiceId as string,
      quantity: quantity as number,
      unitPrice: unitPrice as number,
      taxRate: taxRate as number,
      discountPct: discountPct as number,
      positionType: positionType as string,
    }),
  );

  // All payments. Small flat stream.
  const payments = useQuery(
    `SELECT payments.invoice_id, payments.amount FROM payments`,
    ([invoiceId, amount]) => ({
      invoiceId: invoiceId as string,
      amount: amount as number,
    }),
  );

  const totals = React.useMemo<CustomerTotals[]>(() => {
    const invoiceToCustomer = new Map<string, string>();
    const invoiceCountByCustomer = new Map<string, number>();
    for (const inv of invoices) {
      invoiceToCustomer.set(inv.id, inv.customerId);
      invoiceCountByCustomer.set(
        inv.customerId,
        (invoiceCountByCustomer.get(inv.customerId) ?? 0) + 1,
      );
    }

    const grossByCustomer = new Map<string, number>();
    for (const p of positions) {
      if (p.positionType !== 'service' && p.positionType !== 'product') continue;
      const customerId = invoiceToCustomer.get(p.invoiceId);
      if (customerId === undefined) continue;
      const raw = (p.quantity * p.unitPrice) / 1000;
      const afterDisc = Math.round((raw * (10000 - p.discountPct)) / 10000);
      const gross = Math.round((afterDisc * (10000 + p.taxRate)) / 10000);
      grossByCustomer.set(customerId, (grossByCustomer.get(customerId) ?? 0) + gross);
    }

    const paidByCustomer = new Map<string, number>();
    for (const pay of payments) {
      const customerId = invoiceToCustomer.get(pay.invoiceId);
      if (customerId === undefined) continue;
      paidByCustomer.set(customerId, (paidByCustomer.get(customerId) ?? 0) + pay.amount);
    }

    const out: CustomerTotals[] = [];
    for (const [customerId, invoiceCount] of invoiceCountByCustomer) {
      out.push({
        customerId,
        gross: grossByCustomer.get(customerId) ?? 0,
        paid: paidByCustomer.get(customerId) ?? 0,
        invoiceCount,
      });
    }
    out.sort((a, b) => b.gross - a.gross);
    return out;
  }, [invoices, positions, payments]);

  const top = totals.slice(0, TOP_N);
  const remaining = Math.max(0, totals.length - TOP_N);

  return (
    <Card className="flex flex-col border-border shadow-none">
      <CardHeader className="flex flex-row items-center justify-between space-y-0 p-4">
        <CardTitle className="text-sm font-semibold">Top-Kunden</CardTitle>
        <Users className="h-3.5 w-3.5 text-muted-foreground" aria-hidden />
      </CardHeader>
      <CardContent className="p-2 pt-0">
        {top.length === 0 ? (
          <div className="p-6 text-center text-sm text-muted-foreground">
            Noch keine Umsätze
          </div>
        ) : (
          <ul className="flex flex-col">
            {top.map((t) => (
              <TopCustomerRow
                key={t.customerId}
                customerId={t.customerId}
                gross={t.gross}
                paid={t.paid}
                invoiceCount={t.invoiceCount}
              />
            ))}
          </ul>
        )}
        {remaining > 0 && (
          <div className="px-3 pb-1 pt-2 text-xs text-muted-foreground">
            {remaining === 1 ? '1 weiterer Kunde' : `${remaining} weitere Kunden`}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

interface TopCustomerRowProps {
  customerId: string;
  gross: number;
  paid: number;
  invoiceCount: number;
}

const TopCustomerRow = React.memo(function TopCustomerRow({
  customerId,
  gross,
  paid,
  invoiceCount,
}: TopCustomerRowProps) {
  const status = computePaymentStatus(gross, paid);
  return (
    <li>
      <Link
        to="/customers/$customerId"
        params={{ customerId }}
        className={cn(
          'flex h-12 items-center gap-3 rounded-md px-3 transition-colors',
          'hover:bg-muted/50',
        )}
      >
        <CustomerIdentity customerId={customerId} invoiceCount={invoiceCount} />
        <div className="flex shrink-0 items-center gap-2">
          <span className="text-sm font-semibold tabular-nums">
            {formatEuro(gross)}
          </span>
          <PaymentStatusBadge status={status} />
        </div>
      </Link>
    </li>
  );
});

const CustomerIdentity = React.memo(function CustomerIdentity({
  customerId,
  invoiceCount,
}: {
  customerId: string;
  invoiceCount: number;
}) {
  const rows = useQuery(
    `SELECT customers.name FROM customers WHERE customers.id = UUID '${customerId}'`,
    ([name]) => name as string,
  );
  const name = rows[0];
  const initials = name ? initialsFor(name) : '';

  return (
    <div className="flex min-w-0 flex-1 items-center gap-3">
      <Avatar className="h-7 w-7">
        <AvatarFallback className="text-[10px] font-semibold uppercase">
          {initials || '·'}
        </AvatarFallback>
      </Avatar>
      <div className="flex min-w-0 flex-col leading-tight">
        {name === undefined ? (
          <Skeleton className="h-3.5 w-24" />
        ) : (
          <span className="truncate text-sm font-medium">{name || '—'}</span>
        )}
        <span className="truncate text-xs text-muted-foreground">
          {invoiceCount === 1 ? '1 Rechnung' : `${invoiceCount} Rechnungen`}
        </span>
      </div>
    </div>
  );
});

function initialsFor(name: string): string {
  const parts = name.trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) return '';
  if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
  return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
}
