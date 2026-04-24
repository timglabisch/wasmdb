import { useState } from 'react';
import { Search, Filter } from 'lucide-react';
import { PageHeader, PageBody } from '@/shared/layout/AppShell';
import { Input } from '@/components/ui/input';
import { Card, CardContent } from '@/components/ui/card';
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from '@/components/ui/select';
import {
  Table, TableBody, TableHead, TableHeader, TableRow,
} from '@/components/ui/table';
import { useAsyncQuery } from '@/wasm';
import { DOC_TYPE_LABEL, STATUS_LABEL } from '@/shared/lib/status';
import { InvoiceListRow } from '@/features/invoice/components/InvoiceListRow';
import { NewInvoiceDialog } from '@/features/invoice/components/NewInvoiceDialog';

const DOC_TYPES = ['invoice', 'offer', 'credit_note', 'delivery_note', 'proforma'];
const STATUSES = ['draft', 'sent', 'paid', 'cancelled'];

interface InvoiceListItem {
  id: number;
  docType: string;
  status: string;
}

export default function InvoicesTab() {
  const [docType, setDocType] = useState<string>('all');
  const [status, setStatus] = useState<string>('all');
  const [term, setTerm] = useState('');

  const rows = useAsyncQuery<InvoiceListItem>(
    'SELECT invoices.id, invoices.doc_type, invoices.status FROM invoices.all() ORDER BY invoices.date_issued DESC, invoices.id DESC',
    ([id, dt, st]) => ({
      id: id as number,
      docType: (dt as string) ?? '',
      status: (st as string) ?? '',
    }),
  );

  const ids = rows
    .filter((r) => (docType === 'all' || r.docType === docType)
      && (status === 'all' || r.status === status))
    .map((r) => r.id);

  return (
    <>
      <PageHeader
        title="Rechnungen"
        actions={
          <div className="flex items-center gap-2">
            <div className="relative">
              <Search className="pointer-events-none absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
              <Input
                value={term}
                onChange={(e) => setTerm(e.target.value)}
                placeholder="Nummer suchen..."
                className="h-8 w-56 pl-8 text-sm"
              />
            </div>
            <Select value={docType} onValueChange={setDocType}>
              <SelectTrigger className="h-8 w-[140px]">
                <Filter className="mr-1 h-3.5 w-3.5 text-muted-foreground" />
                <SelectValue placeholder="Typ" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">Alle Typen</SelectItem>
                {DOC_TYPES.map((t) => (
                  <SelectItem key={t} value={t}>{DOC_TYPE_LABEL[t]}</SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select value={status} onValueChange={setStatus}>
              <SelectTrigger className="h-8 w-[140px]">
                <Filter className="mr-1 h-3.5 w-3.5 text-muted-foreground" />
                <SelectValue placeholder="Status" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">Alle Status</SelectItem>
                {STATUSES.map((s) => (
                  <SelectItem key={s} value={s}>{STATUS_LABEL[s]}</SelectItem>
                ))}
              </SelectContent>
            </Select>
            <NewInvoiceDialog />
          </div>
        }
      />
      <PageBody>
        {ids.length === 0 ? (
          <Card>
            <CardContent className="flex flex-col items-center justify-center gap-3 py-16 text-center">
              <div className="text-sm font-medium">Keine Rechnungen</div>
              <div className="text-xs text-muted-foreground">
                Lege einen Entwurf an, um loszulegen.
              </div>
              <div className="mt-2">
                <NewInvoiceDialog />
              </div>
            </CardContent>
          </Card>
        ) : (
          <Card>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Nummer</TableHead>
                  <TableHead>Typ</TableHead>
                  <TableHead>Kunde</TableHead>
                  <TableHead>Datum</TableHead>
                  <TableHead>Fällig</TableHead>
                  <TableHead className="text-right">Brutto</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Zahlung</TableHead>
                  <TableHead />
                </TableRow>
              </TableHeader>
              <TableBody>
                {ids.map((id) => (
                  <InvoiceListRow key={id} invoiceId={id} searchTerm={term} />
                ))}
              </TableBody>
            </Table>
          </Card>
        )}
      </PageBody>
    </>
  );
}
