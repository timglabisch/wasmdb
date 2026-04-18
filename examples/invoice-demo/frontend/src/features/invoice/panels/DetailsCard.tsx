import { memo, useCallback } from 'react';
import { ArrowLeft } from 'lucide-react';
import { Card, CardContent } from '@/components/ui/card';
import {
  Accordion, AccordionContent, AccordionItem, AccordionTrigger,
} from '@/components/ui/accordion';
import { Button } from '@/components/ui/button';
import {
  Field, BlurInput, BlurNumberInput, BlurSelect, BlurTextarea,
} from '@/components/form';
import { useQuery, peekQuery } from '@/wasm';
import { selectById, selectByFk } from '@/queries';
import { usePatchInvoice } from '@/features/invoice/hooks/usePatchInvoice';

const METHOD_OPTIONS = [
  { value: 'transfer', label: 'Überweisung' },
  { value: 'sepa', label: 'SEPA-Lastschrift' },
  { value: 'cash', label: 'Bar' },
  { value: 'card', label: 'Karte' },
];

const CURRENCY_OPTIONS = [
  { value: 'EUR', label: 'EUR' },
  { value: 'USD', label: 'USD' },
  { value: 'CHF', label: 'CHF' },
  { value: 'GBP', label: 'GBP' },
];

const LANGUAGE_OPTIONS = [
  { value: 'de', label: 'Deutsch' },
  { value: 'en', label: 'Englisch' },
];

export function DetailsCard({ invoiceId }: { invoiceId: number }) {
  return (
    <Card>
      <CardContent className="px-5 py-0">
        <Accordion type="multiple" defaultValue={['payment']} className="divide-y">
          <AccordionItem value="payment" className="border-b-0">
            <AccordionTrigger>Zahlung</AccordionTrigger>
            <AccordionContent>
              <PaymentSection invoiceId={invoiceId} />
            </AccordionContent>
          </AccordionItem>
          <AccordionItem value="format" className="border-b-0">
            <AccordionTrigger>Formate</AccordionTrigger>
            <AccordionContent>
              <FormatSection invoiceId={invoiceId} />
            </AccordionContent>
          </AccordionItem>
          <AccordionItem value="addresses" className="border-b-0">
            <AccordionTrigger>Adressen</AccordionTrigger>
            <AccordionContent>
              <AddressSection invoiceId={invoiceId} />
            </AccordionContent>
          </AccordionItem>
          <AccordionItem value="refs" className="border-b-0">
            <AccordionTrigger>Referenzen & Notizen</AccordionTrigger>
            <AccordionContent>
              <RefsSection invoiceId={invoiceId} />
            </AccordionContent>
          </AccordionItem>
        </Accordion>
      </CardContent>
    </Card>
  );
}

/* ---------------- Payment ---------------- */

interface PaymentBits {
  payment_method: string;
  sepa_mandate_id: number;
  customer_id: number;
}

function PaymentSection({ invoiceId }: { invoiceId: number }) {
  const rows = useQuery<PaymentBits>(
    selectById('invoices', 'payment_method, sepa_mandate_id, customer_id', invoiceId),
    ([pm, sm, ci]) => ({
      payment_method: pm as string,
      sepa_mandate_id: sm as number,
      customer_id: ci as number,
    }),
  );
  const patch = usePatchInvoice(invoiceId);
  const bits = rows[0];

  return (
    <div>
      <Field label="Methode">
        <BlurSelect
          value={bits?.payment_method ?? 'transfer'}
          onCommit={(v) => patch({ payment_method: v })}
          options={METHOD_OPTIONS}
        />
      </Field>
      {bits?.payment_method === 'sepa' && (
        <Field label="SEPA-Mandat">
          <SepaMandatePicker
            customerId={bits.customer_id}
            value={bits.sepa_mandate_id}
            onCommit={(id) => patch({ sepa_mandate_id: id })}
          />
        </Field>
      )}
      <CashAllowancePctTile invoiceId={invoiceId} />
      <CashAllowanceDaysTile invoiceId={invoiceId} />
      <DiscountPctTile invoiceId={invoiceId} />
    </div>
  );
}

interface SepaMandate { id: number; mandate_ref: string; status: string }

const SepaMandatePicker = memo(function SepaMandatePicker({
  customerId, value, onCommit,
}: {
  customerId: number;
  value: number;
  onCommit: (id: number) => void;
}) {
  const mandates = useQuery<SepaMandate>(
    selectByFk('sepa_mandates', 'id, mandate_ref, status', 'customer_id', customerId, 'id'),
    ([id, ref, status]) => ({
      id: id as number,
      mandate_ref: ref as string,
      status: status as string,
    }),
  );
  const active = mandates.filter((m) => m.status === 'active');
  const options = [
    { value: '0', label: 'Kein Mandat' },
    ...active.map((m) => ({ value: String(m.id), label: m.mandate_ref || `#${m.id}` })),
  ];
  return (
    <BlurSelect
      value={String(value)}
      onCommit={(next) => onCommit(Number(next))}
      options={options}
    />
  );
});

const CashAllowancePctTile = memo(function CashAllowancePctTile({ invoiceId }: { invoiceId: number }) {
  const rows = useQuery<number>(selectById('invoices', 'cash_allowance_pct', invoiceId), ([v]) => v as number);
  const patch = usePatchInvoice(invoiceId);
  return (
    <Field label="Skonto %" hint="Wert in Basispunkten (100 = 1 %)">
      <BlurNumberInput value={rows[0] ?? 0} onCommit={(v) => patch({ cash_allowance_pct: v })} />
    </Field>
  );
});

const CashAllowanceDaysTile = memo(function CashAllowanceDaysTile({ invoiceId }: { invoiceId: number }) {
  const rows = useQuery<number>(selectById('invoices', 'cash_allowance_days', invoiceId), ([v]) => v as number);
  const patch = usePatchInvoice(invoiceId);
  return (
    <Field label="Skonto Tage">
      <BlurNumberInput value={rows[0] ?? 0} onCommit={(v) => patch({ cash_allowance_days: v })} />
    </Field>
  );
});

const DiscountPctTile = memo(function DiscountPctTile({ invoiceId }: { invoiceId: number }) {
  const rows = useQuery<number>(selectById('invoices', 'discount_pct', invoiceId), ([v]) => v as number);
  const patch = usePatchInvoice(invoiceId);
  return (
    <Field label="Rabatt %" hint="Wert in Basispunkten (100 = 1 %)">
      <BlurNumberInput value={rows[0] ?? 0} onCommit={(v) => patch({ discount_pct: v })} />
    </Field>
  );
});

/* ---------------- Format ---------------- */

function FormatSection({ invoiceId }: { invoiceId: number }) {
  return (
    <div>
      <CurrencyTile invoiceId={invoiceId} />
      <LanguageTile invoiceId={invoiceId} />
    </div>
  );
}

const CurrencyTile = memo(function CurrencyTile({ invoiceId }: { invoiceId: number }) {
  const rows = useQuery<string>(selectById('invoices', 'currency', invoiceId), ([v]) => v as string);
  const patch = usePatchInvoice(invoiceId);
  return (
    <Field label="Währung">
      <BlurSelect
        value={rows[0] ?? 'EUR'}
        onCommit={(v) => patch({ currency: v })}
        options={CURRENCY_OPTIONS}
      />
    </Field>
  );
});

const LanguageTile = memo(function LanguageTile({ invoiceId }: { invoiceId: number }) {
  const rows = useQuery<string>(selectById('invoices', 'language', invoiceId), ([v]) => v as string);
  const patch = usePatchInvoice(invoiceId);
  return (
    <Field label="Sprache">
      <BlurSelect
        value={rows[0] ?? 'de'}
        onCommit={(v) => patch({ language: v })}
        options={LANGUAGE_OPTIONS}
      />
    </Field>
  );
});

/* ---------------- Addresses ---------------- */

function AddressSection({ invoiceId }: { invoiceId: number }) {
  const patch = usePatchInvoice(invoiceId);

  const copyBillingToShipping = useCallback(() => {
    const rows = peekQuery(
      selectById('invoices', 'billing_street, billing_zip, billing_city, billing_country', invoiceId),
    );
    if (rows.length === 0) return;
    const r = rows[0];
    patch({
      shipping_street: r[0] as string,
      shipping_zip: r[1] as string,
      shipping_city: r[2] as string,
      shipping_country: r[3] as string,
    });
  }, [invoiceId, patch]);

  return (
    <div className="grid grid-cols-1 gap-8 md:grid-cols-2">
      <div>
        <div className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
          Rechnungsadresse
        </div>
        <BillingAddressFields invoiceId={invoiceId} />
      </div>
      <div>
        <div className="mb-2 flex items-center justify-between">
          <div className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
            Lieferadresse
          </div>
          <Button variant="ghost" size="sm" onClick={copyBillingToShipping}>
            <ArrowLeft className="h-3.5 w-3.5" />
            aus Rechnungsadresse übernehmen
          </Button>
        </div>
        <ShippingAddressFields invoiceId={invoiceId} />
      </div>
    </div>
  );
}

interface Addr { street: string; zip: string; city: string; country: string }

const BillingAddressFields = memo(function BillingAddressFields({ invoiceId }: { invoiceId: number }) {
  const rows = useQuery<Addr>(
    selectById('invoices', 'billing_street, billing_zip, billing_city, billing_country', invoiceId),
    ([s, z, c, co]) => ({
      street: s as string,
      zip: z as string,
      city: c as string,
      country: co as string,
    }),
  );
  const patch = usePatchInvoice(invoiceId);
  const a = rows[0] ?? { street: '', zip: '', city: '', country: '' };
  return (
    <div>
      <Field label="Straße">
        <BlurInput value={a.street} onCommit={(v) => patch({ billing_street: v })} />
      </Field>
      <Field label="PLZ">
        <BlurInput value={a.zip} onCommit={(v) => patch({ billing_zip: v })} />
      </Field>
      <Field label="Ort">
        <BlurInput value={a.city} onCommit={(v) => patch({ billing_city: v })} />
      </Field>
      <Field label="Land">
        <BlurInput value={a.country} onCommit={(v) => patch({ billing_country: v })} />
      </Field>
    </div>
  );
});

const ShippingAddressFields = memo(function ShippingAddressFields({ invoiceId }: { invoiceId: number }) {
  const rows = useQuery<Addr>(
    selectById('invoices', 'shipping_street, shipping_zip, shipping_city, shipping_country', invoiceId),
    ([s, z, c, co]) => ({
      street: s as string,
      zip: z as string,
      city: c as string,
      country: co as string,
    }),
  );
  const patch = usePatchInvoice(invoiceId);
  const a = rows[0] ?? { street: '', zip: '', city: '', country: '' };
  return (
    <div>
      <Field label="Straße">
        <BlurInput value={a.street} onCommit={(v) => patch({ shipping_street: v })} />
      </Field>
      <Field label="PLZ">
        <BlurInput value={a.zip} onCommit={(v) => patch({ shipping_zip: v })} />
      </Field>
      <Field label="Ort">
        <BlurInput value={a.city} onCommit={(v) => patch({ shipping_city: v })} />
      </Field>
      <Field label="Land">
        <BlurInput value={a.country} onCommit={(v) => patch({ shipping_country: v })} />
      </Field>
    </div>
  );
});

/* ---------------- Refs & Notes ---------------- */

function RefsSection({ invoiceId }: { invoiceId: number }) {
  return (
    <div>
      <ProjectRefTile invoiceId={invoiceId} />
      <ExternalIdTile invoiceId={invoiceId} />
      <NotesTile invoiceId={invoiceId} />
    </div>
  );
}

const ProjectRefTile = memo(function ProjectRefTile({ invoiceId }: { invoiceId: number }) {
  const rows = useQuery<string>(selectById('invoices', 'project_ref', invoiceId), ([v]) => v as string);
  const patch = usePatchInvoice(invoiceId);
  return (
    <Field label="Projekt-Ref">
      <BlurInput value={rows[0] ?? ''} onCommit={(v) => patch({ project_ref: v })} />
    </Field>
  );
});

const ExternalIdTile = memo(function ExternalIdTile({ invoiceId }: { invoiceId: number }) {
  const rows = useQuery<string>(selectById('invoices', 'external_id', invoiceId), ([v]) => v as string);
  const patch = usePatchInvoice(invoiceId);
  return (
    <Field label="Externe ID">
      <BlurInput value={rows[0] ?? ''} onCommit={(v) => patch({ external_id: v })} />
    </Field>
  );
});

const NotesTile = memo(function NotesTile({ invoiceId }: { invoiceId: number }) {
  const rows = useQuery<string>(selectById('invoices', 'notes', invoiceId), ([v]) => v as string);
  const patch = usePatchInvoice(invoiceId);
  return (
    <Field label="Notizen">
      <BlurTextarea value={rows[0] ?? ''} onCommit={(v) => patch({ notes: v })} />
    </Field>
  );
});
