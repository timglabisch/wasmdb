import { memo } from 'react';
import { useParams } from '@tanstack/react-router';
import { PageHeader, PageBody } from '@/shared/layout/AppShell';
import { useQuery, useRequirements, requirements } from '@/wasm';
import { RequirementsGate } from '@/shared/components/RequirementsGate';
import { InvoiceStatusBadge, DocTypeBadge } from '@/shared/lib/status';
import { HeaderActions } from '@/features/invoice/components/HeaderActions';
import { CustomerCard } from '@/features/invoice/panels/CustomerCard';
import { HeaderFieldsCard } from '@/features/invoice/panels/HeaderFieldsCard';
import { PositionsCard } from '@/features/invoice/panels/PositionsCard';
import { DetailsCard } from '@/features/invoice/panels/DetailsCard';
import { PaymentsCard } from '@/features/invoice/panels/PaymentsCard';
import { ActivityCard } from '@/features/invoice/panels/ActivityCard';

export default function InvoiceDetail() {
  const { invoiceId } = useParams({ from: '/invoices/$invoiceId' });
  const { status, error } = useRequirements([
    requirements.invoices.all(),
    requirements.positions.all(),
    requirements.payments.all(),
    requirements.customers.all(),
    requirements.contacts.all(),
    requirements.sepaMandates.all(),
    requirements.activityLog.all(),
    requirements.products.all(),
  ]);

  return (
    <RequirementsGate status={status} error={error} loadingLabel="Lade Beleg…">
      <PageHeader
        title={<HeaderTitle invoiceId={invoiceId} />}
        actions={<HeaderActions invoiceId={invoiceId} />}
      />
      <PageBody className="space-y-3">
        <CustomerCard invoiceId={invoiceId} />
        <HeaderFieldsCard invoiceId={invoiceId} />
        <PositionsCard invoiceId={invoiceId} />
        <DetailsCard invoiceId={invoiceId} />
        <PaymentsCard invoiceId={invoiceId} />
        <ActivityCard invoiceId={invoiceId} />
      </PageBody>
    </RequirementsGate>
  );
}

interface TitleBits { number: string; status: string; doc_type: string }

const HeaderTitle = memo(function HeaderTitle({ invoiceId }: { invoiceId: string }) {
  const rows = useQuery<TitleBits>(
    `SELECT invoices.number, invoices.status, invoices.doc_type ` +
    `FROM invoices WHERE REACTIVE(invoices.id = UUID '${invoiceId}')`,
    ([number, status, doc_type]) => ({
      number: number as string,
      status: status as string,
      doc_type: doc_type as string,
    }),
  );
  const t = rows[0];
  if (!t) return <span>Beleg</span>;
  return (
    <span className="flex items-center gap-2">
      <span>{t.number || `#${invoiceId}`}</span>
      <DocTypeBadge docType={t.doc_type} />
      <InvoiceStatusBadge status={t.status} />
    </span>
  );
});

