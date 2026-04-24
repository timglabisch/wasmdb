import * as React from 'react';
import { Link, useNavigate, useParams } from '@tanstack/react-router';
import {
  ArrowLeft, ArrowLeftRight, ChevronRight, FilePlus, FileText, Mail, MoreHorizontal,
  Phone, Star, Trash2, Users,
} from 'lucide-react';
import { toast } from '@/components/ui/sonner';
import { Avatar, AvatarFallback } from '@/components/ui/avatar';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import {
  Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle,
} from '@/components/ui/dialog';
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuSeparator, DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { PageBody, PageHeader } from '@/shared/layout/AppShell';
import {
  BlurInput, BlurNumberInput, BlurSelect, BlurTextarea, Field,
} from '@/components/form';
import {
  createStream, execute, executeOnStream, flushStream, useQuery,
} from '@/wasm';
import { deleteCustomerCascade } from '@/commands/customer/deleteCustomerCascade';
import { deleteContact } from '@/commands/contact/deleteContact';
import { deleteSepaMandate } from '@/commands/sepaMandate/deleteSepaMandate';
import { logActivity } from '@/commands/activity/logActivity';
import { formatDateISO, formatEuro } from '@/shared/lib/format';
import { InvoiceStatusBadge } from '@/shared/lib/status';
import { useInvoiceGrossCents } from '@/shared/lib/gross';
import { useCreateDraftInvoice } from '@/features/invoice/actions/createDraftInvoice';
import { usePatchCustomer } from './hooks/usePatchCustomer';
import { usePatchContact } from './hooks/usePatchContact';
import { usePatchSepaMandate } from './hooks/usePatchSepaMandate';
import { NewContactDialog } from './components/ContactDialog';
import { NewSepaMandateDialog } from './components/SepaMandateDialog';
import { initialsOf, maskIban } from './lib/util';
import { cn } from '@/lib/cn';

export default function CustomerDetailRoute() {
  const { customerId } = useParams({ from: '/customers/$customerId' });

  const exists = useQuery(
    `SELECT customers.id FROM customers WHERE customers.id = ${customerId}`,
    ([id]) => id as number,
  ).length > 0;

  if (!exists) {
    return <NotFound />;
  }

  return <CustomerDetail customerId={customerId} />;
}

function NotFound() {
  return (
    <>
      <PageHeader title="Kunde nicht gefunden" />
      <PageBody>
        <Card>
          <CardContent className="flex flex-col items-center justify-center gap-4 py-16 text-center">
            <div className="flex h-12 w-12 items-center justify-center rounded-full bg-muted text-muted-foreground">
              <Users className="h-5 w-5" />
            </div>
            <div className="space-y-1">
              <div className="text-sm font-medium">Dieser Kunde existiert nicht (mehr).</div>
              <div className="text-xs text-muted-foreground">
                Der Datensatz wurde möglicherweise gelöscht oder die ID ist ungültig.
              </div>
            </div>
            <Button asChild variant="outline" size="sm">
              <Link to="/customers">
                <ArrowLeft className="h-4 w-4" />
                Zurück zur Kundenliste
              </Link>
            </Button>
          </CardContent>
        </Card>
      </PageBody>
    </>
  );
}

/* -------------------------------------------------------------------------- */
/*  Main detail shell                                                         */
/* -------------------------------------------------------------------------- */

function CustomerDetail({ customerId }: { customerId: number }) {
  const navigate = useNavigate();
  const createDraft = useCreateDraftInvoice();
  const [confirmOpen, setConfirmOpen] = React.useState(false);

  const head = useQuery(
    `SELECT customers.name, customers.company_type, customers.created_at ` +
    `FROM customers WHERE customers.id = ${customerId}`,
    ([name, type, createdAt]) => ({
      name: name as string,
      companyType: type as string,
      createdAt: createdAt as string,
    }),
  )[0];

  const doDelete = async () => {
    const stream = createStream(2);
    executeOnStream(stream, deleteCustomerCascade({ id: customerId }));
    executeOnStream(stream, logActivity({
      entityType: 'customer', entityId: customerId,
      action: 'delete', detail: `Kunde "${head?.name ?? ''}" gelöscht (Kaskade)`,
    }));
    try {
      await flushStream(stream);
      toast.success('Kunde gelöscht');
      navigate({ to: '/customers' });
    } catch (err) {
      toast.error(`Löschen fehlgeschlagen: ${(err as Error).message}`);
    }
  };

  return (
    <>
      <PageHeader
        title={
          <div className="flex min-w-0 items-center gap-3">
            <Link to="/customers" className="shrink-0 text-muted-foreground hover:text-foreground">
              <ArrowLeft className="h-4 w-4" />
            </Link>
            <Avatar className="h-8 w-8 shrink-0">
              <AvatarFallback className="bg-primary/10 text-primary">
                {initialsOf(head?.name ?? '')}
              </AvatarFallback>
            </Avatar>
            <div className="flex min-w-0 items-center gap-2">
              <InlineName customerId={customerId} />
              {head && (
                <Badge variant={head.companyType === 'company' ? 'secondary' : 'outline'}>
                  {head.companyType === 'company' ? 'Firma' : 'Privat'}
                </Badge>
              )}
            </div>
          </div>
        }
        description={
          head?.createdAt
            ? `Kunde seit ${formatDateISO(head.createdAt)}`
            : undefined
        }
        actions={
          <>
            <Button size="sm" onClick={() => { void createDraft(customerId); }}>
              <FilePlus className="h-4 w-4" />
              Neue Rechnung
            </Button>
            <Button asChild variant="outline" size="sm">
              <Link to="/invoices">
                <FileText className="h-4 w-4" />
                Rechnungen ansehen
              </Link>
            </Button>
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="ghost" size="icon" className="h-8 w-8" aria-label="Weitere Aktionen">
                  <MoreHorizontal className="h-4 w-4" />
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end" className="w-48">
                <DropdownMenuItem
                  className="text-destructive focus:text-destructive"
                  onSelect={(e) => { e.preventDefault(); setConfirmOpen(true); }}
                >
                  <Trash2 className="h-4 w-4" />
                  Löschen …
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </>
        }
      />

      <PageBody className="mx-auto w-full max-w-5xl space-y-3">
        <BasisCard customerId={customerId} />
        <TaxPaymentCard customerId={customerId} />
        <AddressGrid customerId={customerId} />
        <BankCard customerId={customerId} />
        <NotesCard customerId={customerId} />
        <ContactsCard customerId={customerId} />
        <SepaMandatesCard customerId={customerId} />
        <InvoicesCard customerId={customerId} />
      </PageBody>

      <Dialog open={confirmOpen} onOpenChange={setConfirmOpen}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Kunde löschen?</DialogTitle>
            <DialogDescription>
              Der Kunde und sämtliche zugehörige Daten — Rechnungen, Positionen, Zahlungen, Kontakte,
              SEPA-Mandate, Serien — werden endgültig entfernt. Diese Aktion kann nicht rückgängig gemacht werden.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="ghost" onClick={() => setConfirmOpen(false)}>Abbrechen</Button>
            <Button variant="destructive" onClick={doDelete}>Endgültig löschen</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}

/* -------------------------------------------------------------------------- */
/*  Inline name (header)                                                      */
/* -------------------------------------------------------------------------- */

const InlineName = React.memo(function InlineName({ customerId }: { customerId: number }) {
  const name = useQuery(
    `SELECT customers.name FROM customers WHERE customers.id = ${customerId}`,
    ([name]) => name as string,
  )[0] ?? '';
  const patch = usePatchCustomer(customerId);
  return (
    <BlurInput
      value={name}
      onCommit={(next) => patch({ name: next })}
      className="h-8 min-w-0 max-w-md border-transparent bg-transparent px-1 text-base font-semibold shadow-none focus-visible:border-input focus-visible:bg-background"
      placeholder="Kundenname"
    />
  );
});

/* -------------------------------------------------------------------------- */
/*  Basis card                                                                */
/* -------------------------------------------------------------------------- */

const BasisCard = React.memo(function BasisCard({ customerId }: { customerId: number }) {
  const row = useQuery(
    `SELECT customers.name, customers.email, customers.company_type, customers.created_at ` +
    `FROM customers WHERE customers.id = ${customerId}`,
    ([name, email, type, createdAt]) => ({
      name: name as string,
      email: email as string,
      companyType: type as string,
      createdAt: createdAt as string,
    }),
  )[0];
  const patch = usePatchCustomer(customerId);
  if (!row) return null;
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm">Basis</CardTitle>
      </CardHeader>
      <CardContent>
        <Field label="Name">
          <BlurInput value={row.name} onCommit={(v) => patch({ name: v })} placeholder="Kundenname" />
        </Field>
        <Field label="E-Mail">
          <BlurInput
            type="email"
            value={row.email}
            onCommit={(v) => patch({ email: v })}
            placeholder="rechnungen@example.com"
          />
        </Field>
        <Field label="Typ">
          <BlurSelect
            value={row.companyType || 'company'}
            onCommit={(v) => patch({ company_type: v })}
            options={[
              { value: 'company', label: 'Firma' },
              { value: 'private', label: 'Privatperson' },
            ]}
          />
        </Field>
        <Field label="Kunde seit">
          <div className="text-sm text-muted-foreground">
            {row.createdAt ? formatDateISO(row.createdAt) : '—'}
          </div>
        </Field>
      </CardContent>
    </Card>
  );
});

/* -------------------------------------------------------------------------- */
/*  Tax & payment                                                             */
/* -------------------------------------------------------------------------- */

const TaxPaymentCard = React.memo(function TaxPaymentCard({ customerId }: { customerId: number }) {
  const row = useQuery(
    `SELECT customers.tax_id, customers.vat_id, customers.payment_terms_days, customers.default_discount_pct ` +
    `FROM customers WHERE customers.id = ${customerId}`,
    ([taxId, vatId, terms, disc]) => ({
      taxId: taxId as string,
      vatId: vatId as string,
      paymentTerms: terms as number,
      defaultDiscount: disc as number,
    }),
  )[0];
  const patch = usePatchCustomer(customerId);
  if (!row) return null;
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm">Steuer &amp; Zahlung</CardTitle>
      </CardHeader>
      <CardContent>
        <Field label="Steuer-Nr.">
          <BlurInput value={row.taxId} onCommit={(v) => patch({ tax_id: v })} placeholder="z. B. 10/123/45678" />
        </Field>
        <Field label="USt-IdNr.">
          <BlurInput value={row.vatId} onCommit={(v) => patch({ vat_id: v })} placeholder="DE123456789" />
        </Field>
        <Field label="Zahlungsziel" hint="in Tagen">
          <BlurNumberInput
            min={0}
            value={row.paymentTerms}
            onCommit={(v) => patch({ payment_terms_days: v })}
          />
        </Field>
        <Field label="Std.-Rabatt" hint="in Basispunkten, 100 bp = 1 %">
          <BlurNumberInput
            min={0}
            value={row.defaultDiscount}
            onCommit={(v) => patch({ default_discount_pct: v })}
          />
        </Field>
      </CardContent>
    </Card>
  );
});

/* -------------------------------------------------------------------------- */
/*  Addresses                                                                 */
/* -------------------------------------------------------------------------- */

const AddressGrid = React.memo(function AddressGrid({ customerId }: { customerId: number }) {
  const billing = useQuery(
    `SELECT customers.billing_street, customers.billing_zip, customers.billing_city, customers.billing_country ` +
    `FROM customers WHERE customers.id = ${customerId}`,
    ([s, z, c, co]) => ({
      street: s as string, zip: z as string, city: c as string, country: co as string,
    }),
  )[0];
  const shipping = useQuery(
    `SELECT customers.shipping_street, customers.shipping_zip, customers.shipping_city, customers.shipping_country ` +
    `FROM customers WHERE customers.id = ${customerId}`,
    ([s, z, c, co]) => ({
      street: s as string, zip: z as string, city: c as string, country: co as string,
    }),
  )[0];
  const patch = usePatchCustomer(customerId);

  if (!billing || !shipping) return null;

  const copyFromBilling = () => {
    patch({
      shipping_street: billing.street,
      shipping_zip: billing.zip,
      shipping_city: billing.city,
      shipping_country: billing.country,
    });
    toast.success('Lieferadresse übernommen');
  };

  return (
    <div className="grid gap-4 lg:grid-cols-2">
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm">Rechnungsadresse</CardTitle>
        </CardHeader>
        <CardContent>
          <Field label="Straße">
            <BlurInput value={billing.street} onCommit={(v) => patch({ billing_street: v })} />
          </Field>
          <Field label="PLZ / Ort">
            <div className="grid grid-cols-[96px_1fr] gap-2">
              <BlurInput value={billing.zip} onCommit={(v) => patch({ billing_zip: v })} />
              <BlurInput value={billing.city} onCommit={(v) => patch({ billing_city: v })} />
            </div>
          </Field>
          <Field label="Land">
            <BlurInput
              value={billing.country}
              onCommit={(v) => patch({ billing_country: v.toUpperCase() })}
              placeholder="DE"
              maxLength={2}
              className="w-24 uppercase"
            />
          </Field>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
          <CardTitle className="text-sm">Lieferadresse</CardTitle>
          <Tooltip>
            <TooltipTrigger asChild>
              <Button size="sm" variant="ghost" className="h-7 gap-1.5 px-2 text-xs" onClick={copyFromBilling}>
                <ArrowLeftRight className="h-3.5 w-3.5" />
                aus Rechnungsadresse
              </Button>
            </TooltipTrigger>
            <TooltipContent>Kopiert die aktuelle Rechnungsadresse.</TooltipContent>
          </Tooltip>
        </CardHeader>
        <CardContent>
          <Field label="Straße">
            <BlurInput value={shipping.street} onCommit={(v) => patch({ shipping_street: v })} />
          </Field>
          <Field label="PLZ / Ort">
            <div className="grid grid-cols-[96px_1fr] gap-2">
              <BlurInput value={shipping.zip} onCommit={(v) => patch({ shipping_zip: v })} />
              <BlurInput value={shipping.city} onCommit={(v) => patch({ shipping_city: v })} />
            </div>
          </Field>
          <Field label="Land">
            <BlurInput
              value={shipping.country}
              onCommit={(v) => patch({ shipping_country: v.toUpperCase() })}
              placeholder="DE"
              maxLength={2}
              className="w-24 uppercase"
            />
          </Field>
        </CardContent>
      </Card>
    </div>
  );
});

/* -------------------------------------------------------------------------- */
/*  Bank                                                                      */
/* -------------------------------------------------------------------------- */

const BankCard = React.memo(function BankCard({ customerId }: { customerId: number }) {
  const row = useQuery(
    `SELECT customers.default_iban, customers.default_bic ` +
    `FROM customers WHERE customers.id = ${customerId}`,
    ([iban, bic]) => ({ iban: iban as string, bic: bic as string }),
  )[0];
  const patch = usePatchCustomer(customerId);
  if (!row) return null;
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm">Bank</CardTitle>
      </CardHeader>
      <CardContent>
        <Field label="IBAN">
          <BlurInput
            value={row.iban}
            onCommit={(v) => patch({ default_iban: v.replace(/\s+/g, '').toUpperCase() })}
            placeholder="DE00 0000 0000 0000 0000 00"
            className="font-mono"
          />
        </Field>
        <Field label="BIC">
          <BlurInput
            value={row.bic}
            onCommit={(v) => patch({ default_bic: v.trim().toUpperCase() })}
            className="font-mono"
          />
        </Field>
      </CardContent>
    </Card>
  );
});

/* -------------------------------------------------------------------------- */
/*  Notes                                                                     */
/* -------------------------------------------------------------------------- */

const NotesCard = React.memo(function NotesCard({ customerId }: { customerId: number }) {
  const row = useQuery(
    `SELECT customers.notes FROM customers WHERE customers.id = ${customerId}`,
    ([notes]) => notes as string,
  )[0];
  const patch = usePatchCustomer(customerId);
  if (row === undefined) return null;
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm">Notizen</CardTitle>
      </CardHeader>
      <CardContent>
        <BlurTextarea
          value={row ?? ''}
          onCommit={(v) => patch({ notes: v })}
          placeholder="Interne Notizen, Hinweise zum Kunden …"
          className="min-h-[96px]"
        />
      </CardContent>
    </Card>
  );
});

/* -------------------------------------------------------------------------- */
/*  Ansprechpartner                                                           */
/* -------------------------------------------------------------------------- */

const ContactsCard = React.memo(function ContactsCard({ customerId }: { customerId: number }) {
  const ids = useQuery(
    `SELECT contacts.id FROM contacts WHERE contacts.customer_id = ${customerId} ` +
    `ORDER BY contacts.is_primary DESC, contacts.name`,
    ([id]) => id as number,
  );
  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle className="text-sm">Ansprechpartner</CardTitle>
        <NewContactDialog customerId={customerId} />
      </CardHeader>
      <CardContent className="p-0">
        {ids.length === 0 ? (
          <div className="px-5 py-8 text-center text-xs text-muted-foreground">
            Noch keine Ansprechpartner hinterlegt.
          </div>
        ) : (
          <ul className="divide-y">
            {ids.map((id) => <ContactRow key={id} contactId={id} />)}
          </ul>
        )}
      </CardContent>
    </Card>
  );
});

const ContactRow = React.memo(function ContactRow({ contactId }: { contactId: number }) {
  const row = useQuery(
    `SELECT contacts.name, contacts.email, contacts.phone, contacts.role, contacts.is_primary ` +
    `FROM contacts WHERE contacts.id = ${contactId}`,
    ([name, email, phone, role, isPrimary]) => ({
      name: name as string,
      email: email as string,
      phone: phone as string,
      role: role as string,
      isPrimary: (isPrimary as number) === 1,
    }),
  )[0];
  const patch = usePatchContact(contactId);

  const togglePrimary = () => {
    patch({ is_primary: row?.isPrimary ? 0 : 1 });
  };

  const remove = async () => {
    await execute(deleteContact({ id: contactId }));
    toast.success('Ansprechpartner entfernt');
  };

  if (!row) return null;

  return (
    <li className="group flex items-center gap-3 px-5 py-3 hover:bg-muted/40">
      <Avatar className="h-8 w-8 shrink-0">
        <AvatarFallback className="bg-muted text-[10px]">
          {initialsOf(row.name)}
        </AvatarFallback>
      </Avatar>
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <span className="truncate text-sm font-medium">{row.name || 'Unbenannt'}</span>
          {row.isPrimary && (
            <Tooltip>
              <TooltipTrigger asChild>
                <Star className="h-3.5 w-3.5 fill-primary text-primary" />
              </TooltipTrigger>
              <TooltipContent>Hauptkontakt</TooltipContent>
            </Tooltip>
          )}
          {row.role && (
            <span className="text-xs text-muted-foreground">· {row.role}</span>
          )}
        </div>
        <div className="mt-0.5 flex items-center gap-3 text-xs text-muted-foreground">
          {row.email && (
            <span className="inline-flex items-center gap-1 truncate">
              <Mail className="h-3 w-3" />
              {row.email}
            </span>
          )}
          {row.phone && (
            <span className="inline-flex items-center gap-1 truncate">
              <Phone className="h-3 w-3" />
              {row.phone}
            </span>
          )}
        </div>
      </div>
      <div className="flex items-center gap-1 opacity-0 transition-opacity group-hover:opacity-100 focus-within:opacity-100">
        <Tooltip>
          <TooltipTrigger asChild>
            <Button variant="ghost" size="icon" className="h-7 w-7" onClick={togglePrimary}>
              <Star className={cn('h-4 w-4', row.isPrimary && 'fill-primary text-primary')} />
            </Button>
          </TooltipTrigger>
          <TooltipContent>
            {row.isPrimary ? 'Als Hauptkontakt entfernen' : 'Als Hauptkontakt markieren'}
          </TooltipContent>
        </Tooltip>
        <Tooltip>
          <TooltipTrigger asChild>
            <Button variant="ghost" size="icon" className="h-7 w-7 text-muted-foreground hover:text-destructive" onClick={remove}>
              <Trash2 className="h-4 w-4" />
            </Button>
          </TooltipTrigger>
          <TooltipContent>Kontakt löschen</TooltipContent>
        </Tooltip>
      </div>
    </li>
  );
});

/* -------------------------------------------------------------------------- */
/*  SEPA-Mandate                                                              */
/* -------------------------------------------------------------------------- */

const SepaMandatesCard = React.memo(function SepaMandatesCard({ customerId }: { customerId: number }) {
  const ids = useQuery(
    `SELECT sepa_mandates.id FROM sepa_mandates WHERE sepa_mandates.customer_id = ${customerId} ` +
    `ORDER BY sepa_mandates.signed_at DESC`,
    ([id]) => id as number,
  );
  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle className="text-sm">SEPA-Mandate</CardTitle>
        <NewSepaMandateDialog customerId={customerId} />
      </CardHeader>
      <CardContent className="p-0">
        {ids.length === 0 ? (
          <div className="px-5 py-8 text-center text-xs text-muted-foreground">
            Noch keine SEPA-Mandate hinterlegt.
          </div>
        ) : (
          <ul className="divide-y">
            {ids.map((id) => <SepaRow key={id} mandateId={id} />)}
          </ul>
        )}
      </CardContent>
    </Card>
  );
});

const SepaRow = React.memo(function SepaRow({ mandateId }: { mandateId: number }) {
  const row = useQuery(
    `SELECT sepa_mandates.mandate_ref, sepa_mandates.iban, sepa_mandates.status, sepa_mandates.signed_at ` +
    `FROM sepa_mandates WHERE sepa_mandates.id = ${mandateId}`,
    ([ref, iban, status, signedAt]) => ({
      ref: ref as string,
      iban: iban as string,
      status: status as string,
      signedAt: signedAt as string,
    }),
  )[0];
  const patch = usePatchSepaMandate(mandateId);
  if (!row) return null;

  const toggleStatus = () => {
    const next = row.status === 'active' ? 'revoked' : 'active';
    patch({ status: next });
    toast.success(next === 'active' ? 'Mandat aktiviert' : 'Mandat widerrufen');
  };

  const remove = async () => {
    await execute(deleteSepaMandate({ id: mandateId }));
    toast.success('Mandat entfernt');
  };

  const active = row.status === 'active';
  return (
    <li className="group flex items-center gap-3 px-5 py-3 hover:bg-muted/40">
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <span className="font-mono text-sm font-medium">{row.ref || '—'}</span>
          <Badge variant={active ? 'success' : 'muted'}>
            {active ? 'aktiv' : 'widerrufen'}
          </Badge>
        </div>
        <div className="mt-0.5 font-mono text-xs text-muted-foreground">
          {maskIban(row.iban)}
          <span className="ml-2">· unterzeichnet {formatDateISO(row.signedAt)}</span>
        </div>
      </div>
      <div className="flex items-center gap-1 opacity-0 transition-opacity group-hover:opacity-100 focus-within:opacity-100">
        <Button variant="ghost" size="sm" className="h-7 text-xs" onClick={toggleStatus}>
          {active ? 'Widerrufen' : 'Aktivieren'}
        </Button>
        <Tooltip>
          <TooltipTrigger asChild>
            <Button variant="ghost" size="icon" className="h-7 w-7 text-muted-foreground hover:text-destructive" onClick={remove}>
              <Trash2 className="h-4 w-4" />
            </Button>
          </TooltipTrigger>
          <TooltipContent>Mandat löschen</TooltipContent>
        </Tooltip>
      </div>
    </li>
  );
});

/* -------------------------------------------------------------------------- */
/*  Invoices                                                                  */
/* -------------------------------------------------------------------------- */

const INVOICE_LIMIT = 10;

const InvoicesCard = React.memo(function InvoicesCard({ customerId }: { customerId: number }) {
  const ids = useQuery(
    `SELECT invoices.id FROM invoices WHERE invoices.customer_id = ${customerId} ` +
    `ORDER BY invoices.date_issued DESC`,
    ([id]) => id as number,
  );
  const shown = ids.slice(0, INVOICE_LIMIT);
  const more = Math.max(0, ids.length - shown.length);
  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle className="text-sm">Rechnungen</CardTitle>
        <span className="text-xs text-muted-foreground">{ids.length} gesamt</span>
      </CardHeader>
      <CardContent className="p-0">
        {ids.length === 0 ? (
          <div className="px-5 py-8 text-center text-xs text-muted-foreground">
            Noch keine Rechnungen für diesen Kunden.
          </div>
        ) : (
          <ul className="divide-y">
            {shown.map((id) => <InvoiceRow key={id} invoiceId={id} />)}
          </ul>
        )}
        {(more > 0 || ids.length > 0) && (
          <div className="flex justify-end border-t px-5 py-2">
            <Button asChild variant="ghost" size="sm" className="gap-1.5 text-xs">
              <Link to="/invoices">
                {more > 0
                  ? `Alle ${ids.length} Rechnungen ansehen`
                  : 'Zur Rechnungsliste'}
                <ChevronRight className="h-3.5 w-3.5" />
              </Link>
            </Button>
          </div>
        )}
      </CardContent>
    </Card>
  );
});

const InvoiceRow = React.memo(function InvoiceRow({ invoiceId }: { invoiceId: number }) {
  const row = useQuery(
    `SELECT invoices.number, invoices.status, invoices.date_issued, invoices.date_due, invoices.doc_type ` +
    `FROM invoices WHERE invoices.id = ${invoiceId}`,
    ([num, status, issued, due, docType]) => ({
      number: num as string,
      status: status as string,
      issued: issued as string,
      due: due as string,
      docType: docType as string,
    }),
  )[0];
  const gross = useInvoiceGrossCents(invoiceId);
  const payments = useQuery(
    `SELECT payments.amount FROM payments WHERE payments.invoice_id = ${invoiceId}`,
    ([amount]) => amount as number,
  );
  const paid = payments.reduce((s, n) => s + n, 0);
  const open = Math.max(0, gross - paid);
  if (!row) return null;

  return (
    <li>
      <Link
        to="/invoices/$invoiceId"
        params={{ invoiceId }}
        className="flex items-center gap-4 px-5 py-3 transition-colors hover:bg-muted/40"
      >
        <span className="w-32 shrink-0 truncate font-mono text-sm font-medium tabular-nums">
          {row.number || '—'}
        </span>
        <InvoiceStatusBadge status={row.status} />
        <span className="hidden text-xs text-muted-foreground sm:inline">
          {formatDateISO(row.issued)}
        </span>
        <span className="ml-auto text-right text-sm tabular-nums">
          <span className="block font-medium">{formatEuro(gross)}</span>
          {open > 0 && row.status !== 'paid' && (
            <span className="block text-xs text-muted-foreground">
              offen {formatEuro(open)}
            </span>
          )}
        </span>
        <ChevronRight className="h-4 w-4 shrink-0 text-muted-foreground" />
      </Link>
    </li>
  );
});
