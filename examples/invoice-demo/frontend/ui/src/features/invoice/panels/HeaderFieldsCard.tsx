import { memo } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Field, BlurInput, BlurDateInput, BlurSelect } from '@/components/form';
import { usePatchInvoice } from '@/features/invoice/hooks/usePatchInvoice';
import { useQuery } from '@/wasm';
import { DOC_TYPE_LABEL, STATUS_LABEL, isOverdue } from '@/shared/lib/status';

const DOC_TYPE_OPTIONS = Object.entries(DOC_TYPE_LABEL).map(([value, label]) => ({ value, label }));
const STATUS_OPTIONS = Object.entries(STATUS_LABEL).map(([value, label]) => ({ value, label }));

/**
 * Each logical field is its own memoized tile that subscribes to exactly one column
 * of `invoices`. That way a status edit doesn't re-render the due-date tile etc.
 */
export function HeaderFieldsCard({ invoiceId }: { invoiceId: string }) {
  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-sm">Kopfdaten</CardTitle>
      </CardHeader>
      <CardContent className="pb-5">
        <div className="grid grid-cols-1 gap-x-6 gap-y-1 md:grid-cols-2">
          <NumberTile invoiceId={invoiceId} />
          <DocTypeTile invoiceId={invoiceId} />
          <StatusTile invoiceId={invoiceId} />
          <IssuedTile invoiceId={invoiceId} />
          <DueTile invoiceId={invoiceId} />
          <ServiceDateTile invoiceId={invoiceId} />
        </div>
      </CardContent>
    </Card>
  );
}

const NumberTile = memo(function NumberTile({ invoiceId }: { invoiceId: string }) {
  const rows = useQuery<string>(
    `SELECT invoices.number FROM invoices WHERE REACTIVE(invoices.id = UUID '${invoiceId}')`,
    ([v]) => v as string,
  );
  const patch = usePatchInvoice(invoiceId);
  const v = rows[0] ?? '';
  return (
    <Field label="Nummer">
      <BlurInput value={v} onCommit={(next) => patch({ number: next })} />
    </Field>
  );
});

const DocTypeTile = memo(function DocTypeTile({ invoiceId }: { invoiceId: string }) {
  const rows = useQuery<string>(
    `SELECT invoices.doc_type FROM invoices WHERE REACTIVE(invoices.id = UUID '${invoiceId}')`,
    ([v]) => v as string,
  );
  const patch = usePatchInvoice(invoiceId);
  return (
    <Field label="Typ">
      <BlurSelect
        value={rows[0] ?? 'invoice'}
        onCommit={(next) => patch({ doc_type: next })}
        options={DOC_TYPE_OPTIONS}
      />
    </Field>
  );
});

const StatusTile = memo(function StatusTile({ invoiceId }: { invoiceId: string }) {
  const rows = useQuery<string>(
    `SELECT invoices.status FROM invoices WHERE REACTIVE(invoices.id = UUID '${invoiceId}')`,
    ([v]) => v as string,
  );
  const patch = usePatchInvoice(invoiceId);
  return (
    <Field label="Status">
      <BlurSelect
        value={rows[0] ?? 'draft'}
        onCommit={(next) => patch({ status: next })}
        options={STATUS_OPTIONS}
      />
    </Field>
  );
});

const IssuedTile = memo(function IssuedTile({ invoiceId }: { invoiceId: string }) {
  const rows = useQuery<string>(
    `SELECT invoices.date_issued FROM invoices WHERE REACTIVE(invoices.id = UUID '${invoiceId}')`,
    ([v]) => v as string,
  );
  const patch = usePatchInvoice(invoiceId);
  return (
    <Field label="Ausgestellt">
      <BlurDateInput value={rows[0] ?? ''} onCommit={(next) => patch({ date_issued: next })} />
    </Field>
  );
});

interface DueBits { date_due: string; status: string }
const DueTile = memo(function DueTile({ invoiceId }: { invoiceId: string }) {
  const rows = useQuery<DueBits>(
    `SELECT invoices.date_due, invoices.status FROM invoices WHERE REACTIVE(invoices.id = UUID '${invoiceId}')`,
    ([date_due, status]) => ({ date_due: date_due as string, status: status as string }),
  );
  const patch = usePatchInvoice(invoiceId);
  const v = rows[0];
  const overdue = v ? isOverdue(v.date_due, v.status) : false;
  return (
    <Field
      label={
        <span className="inline-flex items-center gap-2">
          Fällig
          {overdue && <Badge variant="destructive">überfällig</Badge>}
        </span>
      }
    >
      <BlurDateInput value={v?.date_due ?? ''} onCommit={(next) => patch({ date_due: next })} />
    </Field>
  );
});

const ServiceDateTile = memo(function ServiceDateTile({ invoiceId }: { invoiceId: string }) {
  const rows = useQuery<string>(
    `SELECT invoices.service_date FROM invoices WHERE REACTIVE(invoices.id = UUID '${invoiceId}')`,
    ([v]) => v as string,
  );
  const patch = usePatchInvoice(invoiceId);
  return (
    <Field label="Leistungsdatum">
      <BlurDateInput value={rows[0] ?? ''} onCommit={(next) => patch({ service_date: next })} />
    </Field>
  );
});
