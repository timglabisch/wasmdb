import * as React from 'react';
import { Link } from '@tanstack/react-router';
import {
  Activity as ActivityFallback,
  ArrowRightCircle,
  Ban,
  CheckCircle,
  ChevronRight,
  Copy,
  CreditCard,
  Dot,
  FileText,
  Package,
  Pencil,
  Plus,
  RefreshCw,
  Send,
  Sprout,
  Trash2,
  User,
} from 'lucide-react';
import { useQuery } from '@/wasm';
import { selectById } from '@/queries';
import { Badge } from '@/components/ui/badge';
import type { BadgeProps } from '@/components/ui/badge';
import { cn } from '@/lib/cn';

export interface ActivityRowProps {
  activityId: number;
  searchTerm: string;
}

interface ActivityRow {
  timestamp: string;
  entityType: string;
  entityId: number;
  action: string;
  actor: string;
  detail: string;
}

const timeFmt = new Intl.DateTimeFormat('de-DE', {
  hour: '2-digit',
  minute: '2-digit',
});

function formatTime(iso: string): string {
  if (!iso) return '';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '';
  return timeFmt.format(d);
}

export const ActivityRow = React.memo(function ActivityRow({
  activityId,
  searchTerm,
}: ActivityRowProps) {
  const rows = useQuery(
    selectById(
      'activity_log',
      'timestamp, entity_type, entity_id, action, actor, detail',
      activityId,
    ),
    ([timestamp, entityType, entityId, action, actor, detail]) => ({
      timestamp: timestamp as string,
      entityType: entityType as string,
      entityId: entityId as number,
      action: action as string,
      actor: actor as string,
      detail: detail as string,
    }),
  );
  const row = rows[0];
  if (!row) return null;

  if (
    searchTerm &&
    !(row.detail ?? '').toLowerCase().includes(searchTerm) &&
    !(row.action ?? '').toLowerCase().includes(searchTerm) &&
    !(row.entityType ?? '').toLowerCase().includes(searchTerm)
  ) {
    return null;
  }

  const Icon = pickIcon(row.action, row.entityType);
  const badge = describeAction(row.action);
  const route = routeFor(row.entityType);
  const timeLabel = formatTime(row.timestamp);

  const body = (
    <div className="flex min-w-0 flex-1 items-start gap-3">
      <div
        className={cn(
          'mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-md bg-muted text-muted-foreground',
        )}
        aria-hidden
      >
        <Icon className="h-4 w-4" />
      </div>
      <div className="min-w-0 flex-1">
        <div className="flex items-baseline gap-1.5 text-sm">
          <EntityLabel entityType={row.entityType} entityId={row.entityId} />
          <span className="text-muted-foreground">·</span>
          <span className="truncate text-muted-foreground">
            {actionSentence(row.action, row.entityType)}
          </span>
        </div>
        {row.detail && (
          <p className="mt-0.5 truncate text-xs text-muted-foreground/90">
            {row.detail}
          </p>
        )}
      </div>
      <div className="flex shrink-0 items-center gap-2 pt-0.5">
        <Badge variant={badge.variant} className="font-mono text-[10px] uppercase tracking-wide">
          {badge.label}
        </Badge>
        <span className="w-10 text-right text-[11px] tabular-nums text-muted-foreground">
          {timeLabel}
        </span>
        <ChevronRight
          className={cn(
            'h-3.5 w-3.5 text-muted-foreground/40 transition-opacity',
            route ? 'opacity-0 group-hover:opacity-100' : 'opacity-0',
          )}
          aria-hidden
        />
      </div>
    </div>
  );

  const rowClass = cn(
    'group flex items-start gap-3 rounded-md px-2 py-2 transition-colors',
    route ? 'hover:bg-muted/60' : 'cursor-default',
  );

  if (!route) {
    return (
      <li>
        <div className={rowClass}>{body}</div>
      </li>
    );
  }

  return (
    <li>
      {route.type === 'invoice' ? (
        <Link
          to="/invoices/$invoiceId"
          params={{ invoiceId: row.entityId }}
          className={rowClass}
        >
          {body}
        </Link>
      ) : route.type === 'customer' ? (
        <Link
          to="/customers/$customerId"
          params={{ customerId: row.entityId }}
          className={rowClass}
        >
          {body}
        </Link>
      ) : route.type === 'product' ? (
        <Link
          to="/products/$productId"
          params={{ productId: row.entityId }}
          className={rowClass}
        >
          {body}
        </Link>
      ) : route.type === 'recurring' ? (
        <Link
          to="/recurring/$recurringId"
          params={{ recurringId: row.entityId }}
          className={rowClass}
        >
          {body}
        </Link>
      ) : (
        <div className={rowClass}>{body}</div>
      )}
    </li>
  );
});

// ─── Icon mapping ─────────────────────────────────────────────────────────

type IconCmp = React.ComponentType<{ className?: string }>;

function pickIcon(action: string, entityType: string): IconCmp {
  switch (action) {
    case 'create':
      return Plus;
    case 'credit_note_created':
      return Plus;
    case 'delete':
    case 'delete_cascade':
      return Trash2;
    case 'mark_sent':
      return Send;
    case 'mark_paid':
    case 'payment_received':
      return CheckCircle;
    case 'seed':
      return Sprout;
    case 'update':
      return Pencil;
    case 'convert_to_invoice':
      return ArrowRightCircle;
    case 'duplicate':
      return Copy;
    case 'cancelled':
      return Ban;
  }
  // Recurring runs are often logged with no standardized keyword.
  if (action.includes('recurring') || action.includes('run')) return RefreshCw;
  if (action.endsWith('_created')) return Plus;

  switch (entityType) {
    case 'invoice':
      return FileText;
    case 'customer':
      return User;
    case 'product':
      return Package;
    case 'recurring':
      return RefreshCw;
    case 'payment':
      return CheckCircle;
    case 'sepa':
      return CreditCard;
  }
  return action ? ActivityFallback : Dot;
}

// ─── Action → Badge mapping ───────────────────────────────────────────────

interface BadgeDesc {
  label: string;
  variant: BadgeProps['variant'];
}

const ACTION_LABELS: Record<string, string> = {
  create: 'Erstellt',
  update: 'Geändert',
  delete: 'Gelöscht',
  delete_cascade: 'Gelöscht',
  seed: 'Seed',
  mark_sent: 'Versendet',
  mark_paid: 'Bezahlt',
  payment_received: 'Zahlung',
  convert_to_invoice: 'Umgewandelt',
  duplicate: 'Dupliziert',
  credit_note_created: 'Gutschrift',
  cancelled: 'Storniert',
};

function describeAction(action: string): BadgeDesc {
  const label = ACTION_LABELS[action] ?? prettifyAction(action);
  let variant: BadgeProps['variant'] = 'secondary';
  if (action === 'delete' || action === 'delete_cascade' || action === 'cancelled') {
    variant = 'destructive';
  } else if (action === 'payment_received' || action === 'mark_paid') {
    variant = 'success';
  } else if (action === 'mark_sent') {
    variant = 'warning';
  } else if (action === 'seed') {
    variant = 'muted';
  }
  return { label, variant };
}

function prettifyAction(action: string): string {
  if (!action) return '—';
  return action.replace(/_/g, ' ');
}

const ENTITY_LABEL_DE: Record<string, string> = {
  customer: 'Kunde',
  invoice: 'Rechnung',
  payment: 'Zahlung',
  recurring: 'Serie',
  product: 'Produkt',
  sepa: 'SEPA-Mandat',
};

function actionSentence(action: string, entityType: string): string {
  const subject = ENTITY_LABEL_DE[entityType] ?? entityType ?? 'Eintrag';
  switch (action) {
    case 'create':
      return `${subject} angelegt`;
    case 'update':
      return `${subject} bearbeitet`;
    case 'delete':
      return `${subject} gelöscht`;
    case 'delete_cascade':
      return `${subject} inkl. Abhängigkeiten gelöscht`;
    case 'seed':
      return `${subject} per Seed angelegt`;
    case 'mark_sent':
      return `${subject} als versendet markiert`;
    case 'mark_paid':
      return `${subject} als bezahlt markiert`;
    case 'payment_received':
      return 'Zahlung verbucht';
    case 'convert_to_invoice':
      return `${subject} in Rechnung umgewandelt`;
    case 'duplicate':
      return `${subject} dupliziert`;
    case 'credit_note_created':
      return 'Gutschrift erstellt';
    case 'cancelled':
      return `${subject} storniert`;
    default:
      return `${subject} · ${prettifyAction(action)}`;
  }
}

// ─── Route mapping ────────────────────────────────────────────────────────

type EntityRoute =
  | { type: 'invoice' }
  | { type: 'customer' }
  | { type: 'product' }
  | { type: 'recurring' };

function routeFor(entityType: string): EntityRoute | null {
  switch (entityType) {
    case 'invoice':
      return { type: 'invoice' };
    case 'customer':
      return { type: 'customer' };
    case 'product':
      return { type: 'product' };
    case 'recurring':
      return { type: 'recurring' };
    // payment + sepa have no dedicated detail route
    default:
      return null;
  }
}

// ─── Entity label (per-type subscription) ─────────────────────────────────

interface EntityLabelProps {
  entityType: string;
  entityId: number;
}

const EntityLabel = React.memo(function EntityLabel({
  entityType,
  entityId,
}: EntityLabelProps) {
  switch (entityType) {
    case 'invoice':
      return <InvoiceLabel entityId={entityId} />;
    case 'customer':
      return <CustomerLabel entityId={entityId} />;
    case 'product':
      return <ProductLabel entityId={entityId} />;
    case 'recurring':
      return <RecurringLabel entityId={entityId} />;
    default:
      return <FallbackLabel entityType={entityType} entityId={entityId} />;
  }
});

function LabelText({ children }: { children: React.ReactNode }) {
  return (
    <span className="truncate font-mono text-sm font-semibold text-foreground">
      {children}
    </span>
  );
}

function DeletedLabel({ entityId }: { entityId: number }) {
  return (
    <span className="truncate font-mono text-sm font-semibold text-muted-foreground line-through">
      #{entityId}
    </span>
  );
}

const InvoiceLabel = React.memo(function InvoiceLabel({
  entityId,
}: {
  entityId: number;
}) {
  const rows = useQuery(
    selectById('invoices', 'number', entityId),
    ([number]) => number as string,
  );
  if (rows.length === 0) return <DeletedLabel entityId={entityId} />;
  const number = rows[0];
  return <LabelText>{number || `#${entityId}`}</LabelText>;
});

const CustomerLabel = React.memo(function CustomerLabel({
  entityId,
}: {
  entityId: number;
}) {
  const rows = useQuery(
    selectById('customers', 'name', entityId),
    ([name]) => name as string,
  );
  if (rows.length === 0) return <DeletedLabel entityId={entityId} />;
  const name = rows[0];
  return (
    <span className="truncate text-sm font-semibold text-foreground">
      {name || `#${entityId}`}
    </span>
  );
});

const ProductLabel = React.memo(function ProductLabel({
  entityId,
}: {
  entityId: number;
}) {
  const rows = useQuery(
    selectById('products', 'sku, name', entityId),
    ([sku, name]) => ({ sku: sku as string, name: name as string }),
  );
  if (rows.length === 0) return <DeletedLabel entityId={entityId} />;
  const p = rows[0];
  return (
    <span className="truncate text-sm font-semibold text-foreground">
      {p.sku ? <span className="font-mono">{p.sku}</span> : `#${entityId}`}
      {p.name ? <span className="ml-1.5 font-normal text-muted-foreground">· {p.name}</span> : null}
    </span>
  );
});

const RecurringLabel = React.memo(function RecurringLabel({
  entityId,
}: {
  entityId: number;
}) {
  const rows = useQuery(
    selectById('recurring_invoices', 'template_name', entityId),
    ([name]) => name as string,
  );
  if (rows.length === 0) return <DeletedLabel entityId={entityId} />;
  const name = rows[0];
  return (
    <span className="truncate text-sm font-semibold text-foreground">
      {name || `#${entityId}`}
    </span>
  );
});

function FallbackLabel({
  entityType,
  entityId,
}: {
  entityType: string;
  entityId: number;
}) {
  const label = ENTITY_LABEL_DE[entityType] ?? entityType ?? 'Eintrag';
  return (
    <span className="truncate text-sm font-semibold text-foreground">
      {label} <span className="font-mono text-muted-foreground">#{entityId}</span>
    </span>
  );
}

