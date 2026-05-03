import { memo, useCallback, useMemo, useState } from 'react';
import { Link } from '@tanstack/react-router';
import { ExternalLink, Search, UserPlus, UserX, Repeat2, X } from 'lucide-react';
import { Card, CardContent } from '@/components/ui/card';
import { Avatar, AvatarFallback } from '@/components/ui/avatar';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { ScrollArea } from '@/components/ui/scroll-area';
import { toast } from '@/components/ui/sonner';
import { useQuery } from '@wasmdb/client';
import { assignCustomer } from '@/features/invoice/actions/assignCustomer';

interface InvoiceFk { customer_id: string | null }

/**
 * Invoice's customer slot. Subscribes to `invoices.customer_id` only and
 * renders one of three states:
 *   - no customer   → inline picker (search + list)
 *   - customer set  → read view with actions (switch / detach)
 *   - switching     → inline picker pre-opened on top
 */
export const CustomerCard = memo(function CustomerCard({ invoiceId }: { invoiceId: string }) {
  const rows = useQuery<InvoiceFk>(
    `SELECT invoices.customer_id FROM invoices WHERE REACTIVE(invoices.id = UUID '${invoiceId}')`,
    ([customer_id]) => ({ customer_id: (customer_id as string | null) ?? null }),
  );
  const customerId = rows[0]?.customer_id ?? null;
  const [picking, setPicking] = useState(false);

  const onPick = useCallback(async (newId: string) => {
    setPicking(false);
    await assignCustomer(invoiceId, newId);
    toast.success(newId ? 'Kunde zugewiesen' : 'Kunde entfernt');
  }, [invoiceId]);

  const onDetach = useCallback(async () => {
    await assignCustomer(invoiceId, null);
    toast.success('Kunde entfernt');
  }, [invoiceId]);

  const showPicker = !customerId || picking;

  return (
    <Card>
      <CardContent className="p-0">
        {customerId === null || showPicker ? (
          <CustomerPicker
            currentId={customerId}
            onPick={onPick}
            onCancel={picking ? () => setPicking(false) : undefined}
          />
        ) : (
          <CustomerBody
            customerId={customerId}
            onSwitch={() => setPicking(true)}
            onDetach={onDetach}
          />
        )}
      </CardContent>
    </Card>
  );
});

interface CustomerDisplay {
  name: string;
  email: string;
  billing_street: string;
  billing_zip: string;
  billing_city: string;
  billing_country: string;
  payment_terms_days: number;
}

const CustomerBody = memo(function CustomerBody({
  customerId, onSwitch, onDetach,
}: {
  customerId: string;
  onSwitch: () => void;
  onDetach: () => void;
}) {
  const rows = useQuery<CustomerDisplay>(
    `SELECT customers.name, customers.email, customers.billing_street, customers.billing_zip, ` +
    `customers.billing_city, customers.billing_country, customers.payment_terms_days ` +
    `FROM customers WHERE REACTIVE(customers.id = UUID '${customerId}')`,
    ([name, email, st, zip, city, country, terms]) => ({
      name: name as string,
      email: email as string,
      billing_street: st as string,
      billing_zip: zip as string,
      billing_city: city as string,
      billing_country: country as string,
      payment_terms_days: terms as number,
    }),
  );
  const c = rows[0];
  if (!c) {
    return (
      <div className="flex items-center justify-between gap-3 px-4 py-3">
        <div className="text-xs text-muted-foreground">Kunde nicht gefunden.</div>
        <Button variant="outline" size="sm" onClick={onDetach}>
          <UserX className="h-3.5 w-3.5" /> Entfernen
        </Button>
      </div>
    );
  }
  const initials = c.name.split(' ').map((s) => s[0]).filter(Boolean).slice(0, 2).join('').toUpperCase();
  return (
    <div className="flex items-start gap-3 px-4 py-3">
      <Avatar className="h-9 w-9">
        <AvatarFallback className="text-xs">{initials || '?'}</AvatarFallback>
      </Avatar>
      <div className="min-w-0 flex-1 space-y-0.5">
        <div className="flex items-center gap-2">
          <h2 className="truncate text-[13px] font-semibold leading-tight">{c.name}</h2>
          {c.email && (
            <span className="truncate text-xs text-muted-foreground">{c.email}</span>
          )}
        </div>
        <div className="text-xs text-muted-foreground">
          {c.billing_street && <span>{c.billing_street} · </span>}
          {c.billing_zip && <span>{c.billing_zip} </span>}
          {c.billing_city && <span>{c.billing_city}</span>}
          {c.billing_country && <span> · {c.billing_country}</span>}
        </div>
        <div className="text-xs text-muted-foreground">
          Zahlungsziel: {c.payment_terms_days} Tage
        </div>
      </div>
      <div className="flex items-center gap-1">
        <Button variant="ghost" size="sm" onClick={onSwitch}>
          <Repeat2 className="h-3.5 w-3.5" /> Wechseln
        </Button>
        <Button variant="ghost" size="sm" onClick={onDetach}>
          <UserX className="h-3.5 w-3.5" /> Entfernen
        </Button>
        <Button asChild variant="ghost" size="icon" className="h-7 w-7">
          <Link to="/customers/$customerId" params={{ customerId }}>
            <ExternalLink className="h-3.5 w-3.5" />
            <span className="sr-only">Kunde öffnen</span>
          </Link>
        </Button>
      </div>
    </div>
  );
});

interface CustomerOption {
  id: string;
  name: string;
  email: string;
  city: string;
}

const CustomerPicker = memo(function CustomerPicker({
  currentId, onPick, onCancel,
}: {
  currentId: string | null;
  onPick: (id: string) => void;
  onCancel?: () => void;
}) {
  const [term, setTerm] = useState('');
  const customers = useQuery<CustomerOption>(
    'SELECT REACTIVE(customers.id), customers.id, customers.name, customers.email, customers.billing_city FROM customers ORDER BY customers.name',
    ([_r, id, name, email, city]) => ({
      id: id as string,
      name: name as string,
      email: email as string,
      city: city as string,
    }),
  );
  const filtered = useMemo(() => {
    const t = term.trim().toLowerCase();
    if (!t) return customers;
    return customers.filter(
      (c) =>
        c.name.toLowerCase().includes(t) ||
        c.email.toLowerCase().includes(t) ||
        c.city.toLowerCase().includes(t),
    );
  }, [customers, term]);

  return (
    <div>
      <div className="flex items-center gap-2 border-b px-3 py-2">
        <UserPlus className="h-3.5 w-3.5 text-muted-foreground" />
        <span className="text-[11px] font-medium uppercase tracking-wider text-muted-foreground">
          Kunde auswählen
        </span>
        {onCancel && (
          <Button variant="ghost" size="icon" className="ml-auto h-6 w-6" onClick={onCancel}>
            <X className="h-3.5 w-3.5" />
            <span className="sr-only">Abbrechen</span>
          </Button>
        )}
      </div>
      <div className="relative border-b">
        <Search className="pointer-events-none absolute left-3 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
        <Input
          autoFocus
          value={term}
          onChange={(e) => setTerm(e.target.value)}
          placeholder="Kunde suchen nach Name, E-Mail oder Stadt..."
          className="h-9 rounded-none border-0 pl-9 focus-visible:ring-0"
        />
      </div>
      <ScrollArea className="max-h-64">
        {filtered.length === 0 ? (
          <div className="px-4 py-6 text-center text-xs text-muted-foreground">
            Keine Kunden gefunden.
          </div>
        ) : (
          <ul className="divide-y">
            {filtered.map((c) => (
              <li key={c.id}>
                <button
                  type="button"
                  onClick={() => onPick(c.id)}
                  className={
                    'flex w-full items-center justify-between gap-3 px-3 py-1.5 text-left transition-colors hover:bg-accent ' +
                    (c.id === currentId ? 'bg-accent/40' : '')
                  }
                >
                  <div className="min-w-0">
                    <div className="truncate text-[13px] font-medium leading-tight">{c.name}</div>
                    {(c.email || c.city) && (
                      <div className="truncate text-[11px] text-muted-foreground">
                        {c.email}{c.email && c.city && ' · '}{c.city}
                      </div>
                    )}
                  </div>
                  {c.id === currentId && (
                    <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
                      aktuell
                    </span>
                  )}
                </button>
              </li>
            ))}
          </ul>
        )}
      </ScrollArea>
    </div>
  );
});
