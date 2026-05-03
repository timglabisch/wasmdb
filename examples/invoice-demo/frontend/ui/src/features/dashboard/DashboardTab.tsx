import * as React from 'react';
import {
  Users,
  FileText,
  Clock,
  AlertTriangle,
  Wallet,
  Coins,
} from 'lucide-react';
import { useQuery, useRequirements } from '@wasmdb/client';
import { requirements } from '@/generated/requirements';
import { PageHeader, PageBody } from '@/shared/layout/AppShell';
import { RequirementsGate } from '@/shared/components/RequirementsGate';
import { formatEuro } from '@/shared/lib/format';
import { useGlobalGrossCents } from '@/shared/lib/gross';
import { KpiCard } from '@/features/dashboard/KpiCard';
import { OverdueList } from '@/features/dashboard/OverdueList';
import { TopCustomers } from '@/features/dashboard/TopCustomers';

/**
 * Dashboard landing page.
 *
 * Each KPI tile is wrapped in its own subscription so a single mutation only
 * rerenders the tile whose projection actually changed. Lists below use the
 * same per-row pattern — see OverdueList / TopCustomers.
 */
export default function DashboardTab() {
  const { status, error } = useRequirements([
    requirements.customers.customerServer.all(),
    requirements.invoices.invoiceServer.all(),
    requirements.payments.paymentServer.all(),
    requirements.positions.positionServer.all(),
  ]);
  return (
    <>
      <PageHeader
        title="Dashboard"
        description="KPIs, überfällige Rechnungen, Umsatz je Kunde."
      />
      <PageBody>
        <RequirementsGate status={status} error={error} loadingLabel="Lade Dashboard…">
        <div className="flex flex-col gap-6">
          <section
            className="grid gap-4 grid-cols-1 sm:grid-cols-2 md:grid-cols-3 xl:grid-cols-6"
            aria-label="Kennzahlen"
          >
            <CustomersKpi />
            <InvoicesKpi />
            <OpenKpi />
            <OverdueKpi />
            <ReceivedKpi />
            <OutstandingKpi />
          </section>

          <section className="grid gap-4 grid-cols-1 lg:grid-cols-3">
            <div className="lg:col-span-2 min-w-0">
              <OverdueList />
            </div>
            <div className="min-w-0">
              <TopCustomers />
            </div>
          </section>
        </div>
        </RequirementsGate>
      </PageBody>
    </>
  );
}

// -------- KPI tiles (one subscription wrapper per tile) ---------------------

const CustomersKpi = React.memo(function CustomersKpi() {
  const rows = useQuery(
    'SELECT REACTIVE(customers.id), COUNT(customers.id) FROM customers',
    ([_r, n]) => n as number,
  );
  const count = rows[0] ?? 0;
  return (
    <KpiCard
      label="Kunden"
      value={count.toLocaleString('de-DE')}
      hint="Stammdaten"
      icon={Users}
    />
  );
});

const InvoicesKpi = React.memo(function InvoicesKpi() {
  const rows = useQuery(
    `SELECT REACTIVE(invoices.id), COUNT(invoices.id) FROM invoices WHERE invoices.doc_type = 'invoice'`,
    ([_r, n]) => n as number,
  );
  const count = rows[0] ?? 0;
  return (
    <KpiCard
      label="Rechnungen"
      value={count.toLocaleString('de-DE')}
      hint="Alle Belege"
      icon={FileText}
    />
  );
});

const OpenKpi = React.memo(function OpenKpi() {
  const rows = useQuery(
    `SELECT REACTIVE(invoices.id), COUNT(invoices.id) FROM invoices ` +
      `WHERE invoices.doc_type = 'invoice' ` +
      `AND invoices.status IN ('draft', 'sent')`,
    ([_r, n]) => n as number,
  );
  const count = rows[0] ?? 0;
  return (
    <KpiCard
      label="Offen"
      value={count.toLocaleString('de-DE')}
      hint="Entwurf oder versendet"
      icon={Clock}
      tone={count > 0 ? 'warning' : 'muted'}
    />
  );
});

const OverdueKpi = React.memo(function OverdueKpi() {
  const today = new Date().toISOString().slice(0, 10);
  const rows = useQuery(
    `SELECT REACTIVE(invoices.id), COUNT(invoices.id) FROM invoices ` +
      `WHERE invoices.doc_type = 'invoice' ` +
      `AND invoices.status IN ('draft', 'sent') ` +
      `AND invoices.date_due < '${today}'`,
    ([_r, n]) => n as number,
  );
  const count = rows[0] ?? 0;
  return (
    <KpiCard
      label="Überfällig"
      value={count.toLocaleString('de-DE')}
      hint="Fälligkeit überschritten"
      icon={AlertTriangle}
      tone={count > 0 ? 'destructive' : 'muted'}
    />
  );
});

const ReceivedKpi = React.memo(function ReceivedKpi() {
  const rows = useQuery(
    'SELECT REACTIVE(payments.id), SUM(payments.amount) FROM payments',
    ([_r, n]) => n as number,
  );
  const cents = rows[0] ?? 0;
  return (
    <KpiCard
      label="Eingegangen"
      value={formatEuro(cents)}
      hint="Summe aller Zahlungen"
      icon={Wallet}
      tone={cents > 0 ? 'success' : 'muted'}
    />
  );
});

const OutstandingKpi = React.memo(function OutstandingKpi() {
  const gross = useGlobalGrossCents();
  const rows = useQuery(
    'SELECT REACTIVE(payments.id), SUM(payments.amount) FROM payments',
    ([_r, n]) => n as number,
  );
  const paid = rows[0] ?? 0;
  const open = gross - paid;
  return (
    <KpiCard
      label="Offen gesamt"
      value={formatEuro(open)}
      hint="Forderungen aus Rechnungen"
      icon={Coins}
      tone={open > 0 ? 'warning' : 'muted'}
    />
  );
});
