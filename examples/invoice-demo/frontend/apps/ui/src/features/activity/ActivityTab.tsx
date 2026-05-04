import * as React from 'react';
import { Search, Activity as ActivityIcon, Inbox } from 'lucide-react';
import { useQuery, useRequirements } from '@wasmdb/client';
import { requirements } from 'invoice-demo-generated/requirements';
import { PageHeader, PageBody } from '@/shared/layout/AppShell';
import { RequirementsGate } from '@/shared/components/RequirementsGate';
import { Card, CardContent } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { formatDateISO, relativeDaysFromToday } from '@/shared/lib/format';
import { ActivityRow } from './ActivityRow';
import type { EntityTypeFilter } from './types';
import { ENTITY_FILTERS, PAGE_SIZE } from './types';

/**
 * Group activity IDs by local day (YYYY-MM-DD).
 * The caller already receives them in DESC timestamp order, so the grouping
 * preserves the day order.
 */
interface DayBucket {
  day: string;
  ids: string[];
}

function groupByDay(rows: Array<{ id: string; timestamp: string }>): DayBucket[] {
  const buckets: DayBucket[] = [];
  let current: DayBucket | null = null;
  for (const row of rows) {
    const day = (row.timestamp ?? '').slice(0, 10);
    if (!current || current.day !== day) {
      current = { day, ids: [] };
      buckets.push(current);
    }
    current.ids.push(row.id);
  }
  return buckets;
}

function dayLabel(day: string): string {
  if (!day) return '—';
  const diff = relativeDaysFromToday(day);
  if (diff === 0) return 'Heute';
  if (diff === -1) return 'Gestern';
  return formatDateISO(day);
}

export default function ActivityTab() {
  const [entityFilter, setEntityFilter] = React.useState<EntityTypeFilter>('all');
  const [search, setSearch] = React.useState('');
  const [limit, setLimit] = React.useState(PAGE_SIZE);
  const { status, error } = useRequirements([
    requirements.activityLog.activityLogServer.all(),
    requirements.invoices.invoiceServer.all(),
    requirements.customers.customerServer.all(),
    requirements.products.productServer.all(),
    requirements.contacts.contactServer.all(),
    requirements.payments.paymentServer.all(),
    requirements.positions.positionServer.all(),
    requirements.recurring.recurringInvoiceServer.all(),
    requirements.sepaMandates.sepaMandateServer.all(),
  ]);

  // Reset limit whenever the filter changes so results don't look stale.
  React.useEffect(() => {
    setLimit(PAGE_SIZE);
  }, [entityFilter]);

  const sql = React.useMemo(() => {
    const where =
      entityFilter === 'all'
        ? ''
        : `WHERE activity_log.entity_type = '${entityFilter}' `;
    return (
      `SELECT REACTIVE(activity_log.id), activity_log.id, activity_log.timestamp ` +
      `FROM activity_log ` +
      where +
      `ORDER BY activity_log.timestamp DESC, activity_log.id DESC ` +
      `LIMIT ${limit}`
    );
  }, [entityFilter, limit]);

  const rows = useQuery(sql, ([_r, id, timestamp]) => ({
    id: id as string,
    timestamp: timestamp as string,
  }));

  const buckets = React.useMemo(() => groupByDay(rows), [rows]);
  const normalizedSearch = search.trim().toLowerCase();
  const hasSearch = normalizedSearch.length > 0;

  const actions = (
    <>
      <div className="relative">
        <Search className="pointer-events-none absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
        <Input
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Detail durchsuchen…"
          className="h-8 w-56 pl-8 text-xs"
          aria-label="Aktivitäten durchsuchen"
        />
      </div>
      <Select
        value={entityFilter}
        onValueChange={(v) => setEntityFilter(v as EntityTypeFilter)}
      >
        <SelectTrigger className="h-8 w-40 text-xs" aria-label="Typ filtern">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {ENTITY_FILTERS.map((f) => (
            <SelectItem key={f.value} value={f.value} className="text-xs">
              {f.label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </>
  );

  return (
    <>
      <PageHeader
        title="Aktivität"
        description="Chronologischer Log aller Geschäftsvorfälle"
        actions={actions}
      />
      <PageBody>
        <RequirementsGate status={status} error={error} loadingLabel="Lade Aktivität…">
        {rows.length === 0 ? (
          <EmptyState />
        ) : (
          <div className="mx-auto max-w-3xl">
            <div className="flex flex-col">
              {buckets.map((bucket) => (
                <DaySection
                  key={bucket.day}
                  day={bucket.day}
                  ids={bucket.ids}
                  searchTerm={normalizedSearch}
                />
              ))}
            </div>
            {rows.length >= limit && (
              <div className="mt-4 flex justify-center">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setLimit((l) => l + PAGE_SIZE)}
                >
                  Weitere laden
                </Button>
              </div>
            )}
            {hasSearch && (
              <p className="mt-3 text-center text-[11px] text-muted-foreground">
                Suche filtert die geladenen Einträge clientseitig.
              </p>
            )}
          </div>
        )}
        </RequirementsGate>
      </PageBody>
    </>
  );
}

interface DaySectionProps {
  day: string;
  ids: string[];
  searchTerm: string;
}

function DaySection({ day, ids, searchTerm }: DaySectionProps) {
  return (
    <section className="relative">
      <div className="sticky top-0 z-10 -mx-2 flex items-center gap-2 bg-background/90 px-2 py-2 backdrop-blur supports-[backdrop-filter]:bg-background/70">
        <span className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
          {dayLabel(day)}
        </span>
        <span className="h-px flex-1 bg-border" aria-hidden />
        <span className="text-[11px] tabular-nums text-muted-foreground">
          {ids.length}
        </span>
      </div>
      <ul className="flex flex-col">
        {ids.map((id) => (
          <ActivityRow key={id} activityId={id} searchTerm={searchTerm} />
        ))}
      </ul>
    </section>
  );
}

function EmptyState() {
  return (
    <div className="mx-auto max-w-xl">
      <Card>
        <CardContent className="flex flex-col items-center justify-center gap-3 py-12 text-center">
          <div className="flex h-12 w-12 items-center justify-center rounded-full bg-muted text-muted-foreground">
            <Inbox className="h-5 w-5" />
          </div>
          <div className="text-sm font-medium">Noch keine Aktivität</div>
          <p className="max-w-sm text-xs text-muted-foreground">
            Sobald Geschäftsvorfälle entstehen, erscheinen sie hier. Zum Ausprobieren
            kannst du in der Seitenleiste unter{' '}
            <span className="inline-flex items-center gap-1 font-medium text-foreground">
              <ActivityIcon className="h-3 w-3" />
              Dev-Werkzeuge
            </span>{' '}
            die Seed-Daten laden.
          </p>
        </CardContent>
      </Card>
    </div>
  );
}
