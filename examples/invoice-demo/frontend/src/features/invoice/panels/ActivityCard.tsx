import {
  FilePlus, Send, CheckCircle2, Copy, FileMinus2, Ban, Trash2, Pencil, CircleDot,
} from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { useQuery } from '@/wasm';

interface Entry {
  id: number;
  timestamp: string;
  action: string;
  detail: string;
}

/**
 * Relative formatter specific to this card. Kept inline to avoid adding
 * helpers that other pages might need to discover.
 */
function relativeTime(iso: string): string {
  if (!iso) return '';
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return iso;
  const delta = Math.round((t - Date.now()) / 1000);
  const absD = Math.abs(delta);
  const rtf = new Intl.RelativeTimeFormat('de', { numeric: 'auto' });
  if (absD < 60) return rtf.format(Math.round(delta), 'second');
  if (absD < 3600) return rtf.format(Math.round(delta / 60), 'minute');
  if (absD < 86400) return rtf.format(Math.round(delta / 3600), 'hour');
  return rtf.format(Math.round(delta / 86400), 'day');
}

const iconFor = (action: string) => {
  if (action.startsWith('create')) return FilePlus;
  if (action.startsWith('status_sent')) return Send;
  if (action.startsWith('status_paid')) return CheckCircle2;
  if (action.startsWith('duplicate')) return Copy;
  if (action.startsWith('credit_note')) return FileMinus2;
  if (action === 'storno') return Ban;
  if (action === 'delete') return Trash2;
  if (action.startsWith('update') || action.startsWith('edit')) return Pencil;
  return CircleDot;
};

export function ActivityCard({ invoiceId }: { invoiceId: number }) {
  // No generic helper for a 2-col filter, so we write SQL inline.
  const entries = useQuery<Entry>(
    `SELECT activity_log.id, activity_log.timestamp, activity_log.action, activity_log.detail ` +
    `FROM activity_log ` +
    `WHERE activity_log.entity_type = 'invoice' AND activity_log.entity_id = ${invoiceId} ` +
    `ORDER BY activity_log.timestamp DESC, activity_log.id DESC`,
    ([id, ts, action, detail]) => ({
      id: id as number,
      timestamp: ts as string,
      action: action as string,
      detail: detail as string,
    }),
  );

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-sm">Historie</CardTitle>
      </CardHeader>
      <CardContent className="pb-5">
        {entries.length === 0 ? (
          <div className="py-4 text-center text-sm text-muted-foreground">
            Noch keine Aktivitäten.
          </div>
        ) : (
          <ul className="space-y-3">
            {entries.map((e) => {
              const Icon = iconFor(e.action);
              return (
                <li key={e.id} className="flex items-start gap-3">
                  <div className="mt-0.5 flex h-6 w-6 shrink-0 items-center justify-center rounded-full bg-muted text-muted-foreground">
                    <Icon className="h-3.5 w-3.5" />
                  </div>
                  <div className="min-w-0 flex-1">
                    <div className="truncate text-sm">{e.detail}</div>
                    <div className="text-xs text-muted-foreground">{relativeTime(e.timestamp)}</div>
                  </div>
                </li>
              );
            })}
          </ul>
        )}
      </CardContent>
    </Card>
  );
}
