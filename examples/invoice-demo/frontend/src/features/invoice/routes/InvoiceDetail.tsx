import { memo } from 'react';
import { Link, useParams } from '@tanstack/react-router';
import { ArrowLeft } from 'lucide-react';
import { PageHeader, PageBody } from '@/shared/layout/AppShell';
import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { useQuery } from '@/wasm';
import { InvoiceStatusBadge, DocTypeBadge } from '@/shared/lib/status';
import { useInvoiceExists } from '@/features/invoice/hooks/useInvoiceExists';
import { HeaderActions } from '@/features/invoice/components/HeaderActions';
import { CustomerCard } from '@/features/invoice/panels/CustomerCard';
import { HeaderFieldsCard } from '@/features/invoice/panels/HeaderFieldsCard';
import { PositionsCard } from '@/features/invoice/panels/PositionsCard';
import { DetailsCard } from '@/features/invoice/panels/DetailsCard';
import { PaymentsCard } from '@/features/invoice/panels/PaymentsCard';
import { ActivityCard } from '@/features/invoice/panels/ActivityCard';

export default function InvoiceDetail() {
  const { invoiceId } = useParams({ from: '/invoices/$invoiceId' });
  const exists = useInvoiceExists(invoiceId);

  if (!exists) {
    return <NotFound />;
  }

  return (
    <>
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
    </>
  );
}

interface TitleBits { number: string; status: string; doc_type: string }

const HeaderTitle = memo(function HeaderTitle({ invoiceId }: { invoiceId: string }) {
  const rows = useQuery<TitleBits>(
    `SELECT invoices.number, invoices.status, invoices.doc_type ` +
    `FROM invoices WHERE invoices.id = UUID '${invoiceId}'`,
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

function NotFound() {
  return (
    <>
      <PageHeader title="Beleg nicht gefunden" />
      <PageBody>
        <Card>
          <CardContent className="flex flex-col items-center justify-center gap-3 py-12 text-center">
            <div className="text-sm font-medium">Beleg nicht gefunden</div>
            <div className="text-xs text-muted-foreground">
              Dieser Beleg existiert nicht oder wurde gelöscht.
            </div>
            <Button asChild variant="outline" size="sm" className="mt-2">
              <Link to="/invoices">
                <ArrowLeft className="h-4 w-4" /> Zurück zur Liste
              </Link>
            </Button>
          </CardContent>
        </Card>
      </PageBody>
    </>
  );
}
