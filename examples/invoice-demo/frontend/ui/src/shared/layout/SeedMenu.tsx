import { useState } from 'react';
import { Sprout, MoreHorizontal, Trash2 } from 'lucide-react';
import { toast } from '@/components/ui/sonner';
import { Button } from '@/components/ui/button';
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuLabel, DropdownMenuSeparator, DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { createStream, flushStream, nextId } from '@wasmdb/client';
import { executeOnStream } from '@/commands';
import {
  createProduct,
  createCustomer,
  createContact,
  createSepaMandate,
  createInvoice,
  addPosition,
  createPayment,
  createRecurring,
  addRecurringPosition,
  logActivity,
} from '@/generated/InvoiceCommandFactories';

const CUSTOMERS = [
  { name: 'Acme Industries',   email: 'billing@acme.example',     city: 'Hamburg',   zip: '20095' },
  { name: 'Globex Corp',       email: 'ap@globex.example',        city: 'München',   zip: '80331' },
  { name: 'Initech',           email: 'finance@initech.example',  city: 'Berlin',    zip: '10115' },
  { name: 'Soylent Inc',       email: 'accounts@soylent.example', city: 'Köln',      zip: '50667' },
  { name: 'Umbrella Holdings', email: 'invoices@umbrella.example',city: 'Frankfurt', zip: '60311' },
];

const CONTACT_ROLES = ['Geschäftsführer', 'Einkauf', 'Buchhaltung'];

const PRODUCTS = [
  { sku: 'CONS-H',  name: 'Beratungsstunde',   unit: 'h',        price: 12000,  tax: 1900, cost: 6000 },
  { sku: 'LIC-STD', name: 'Software-Lizenz',   unit: 'Stk',      price: 49900,  tax: 1900, cost: 5000 },
  { sku: 'TR-D',    name: 'Schulungstag',      unit: 'Tag',      price: 90000,  tax: 1900, cost: 35000 },
  { sku: 'IMPL',    name: 'Implementierung',   unit: 'Pauschal', price: 150000, tax: 1900, cost: 50000 },
  { sku: 'MAINT-M', name: 'Wartung (Monat)',   unit: 'Monat',    price: 25000,  tax: 1900, cost: 3000 },
  { sku: 'REISE',   name: 'Reisekosten',       unit: 'Pauschal', price: 8500,   tax: 700,  cost: 8500 },
  { sku: 'HW-SRV',  name: 'Server-Hardware',   unit: 'Stk',      price: 250000, tax: 1900, cost: 180000 },
];

const isoDate = (daysFromNow = 0): string => {
  const d = new Date();
  d.setDate(d.getDate() + daysFromNow);
  return d.toISOString().slice(0, 10);
};

const pick = <T,>(arr: T[]): T => arr[Math.floor(Math.random() * arr.length)];

async function seedSample() {
  const stream = createStream(512);

  const productIds: string[] = [];
  for (const p of PRODUCTS) {
    const pid = nextId();
    productIds.push(pid);
    executeOnStream(stream, createProduct({
      id: pid, sku: p.sku, name: p.name,
      description: p.name,
      unit: p.unit, unit_price: p.price, tax_rate: p.tax, cost_price: p.cost,
      active: 1,
    }));
  }
  const productMap = PRODUCTS.map((p, i) => ({ ...p, id: productIds[i] }));

  for (const c of CUSTOMERS) {
    const customerId = nextId();
    executeOnStream(stream, createCustomer({
      id: customerId, name: c.name, email: c.email,
      created_at: isoDate(-90), company_type: 'company',
      tax_id: '',
      vat_id: `DE${Math.floor(100000000 + Math.random() * 899999999)}`,
      payment_terms_days: pick([7, 14, 30]),
      default_discount_pct: 0,
      billing_street: `Hauptstr. ${Math.floor(Math.random() * 200) + 1}`,
      billing_zip: c.zip, billing_city: c.city, billing_country: 'DE',
      shipping_street: `Hauptstr. ${Math.floor(Math.random() * 200) + 1}`,
      shipping_zip: c.zip, shipping_city: c.city, shipping_country: 'DE',
      default_iban: `DE${Math.floor(10 + Math.random() * 89)}1234567890${Math.floor(1000000 + Math.random() * 8999999)}`,
      default_bic: 'GENODEF1S02',
      notes: '',
    }));
    executeOnStream(stream, logActivity({
      entity_type: 'customer', entity_id: customerId,
      action: 'seed', actor: 'demo', detail: `Demo-Kunde "${c.name}" angelegt`,
    }));

    executeOnStream(stream, createSepaMandate({
      id: nextId(), customer_id: customerId,
      mandate_ref: `MAND-${customerId}-2026`,
      iban: `DE${Math.floor(10 + Math.random() * 89)}1234567890${Math.floor(1000000 + Math.random() * 8999999)}`,
      bic: 'GENODEF1S02', holder_name: c.name, signed_at: isoDate(-60),
    }));

    const contactCount = 1 + Math.floor(Math.random() * 2);
    for (let k = 0; k < contactCount; k++) {
      executeOnStream(stream, createContact({
        id: nextId(), customer_id: customerId,
        name: `Kontakt ${k + 1}`,
        email: `kontakt${k + 1}@${c.name.toLowerCase().replace(/[^a-z]/g, '')}.example`,
        phone: `+49 30 ${Math.floor(1000000 + Math.random() * 8999999)}`,
        role: pick(CONTACT_ROLES), is_primary: k === 0 ? 1 : 0,
      }));
    }

    const invCount = 1 + Math.floor(Math.random() * 3);
    for (let i = 0; i < invCount; i++) {
      const invoiceId = nextId();
      const issued = -Math.floor(Math.random() * 60);
      const status = pick(['draft', 'sent', 'paid']);
      executeOnStream(stream, createInvoice({
        id: invoiceId, customer_id: customerId,
        number: `INV-2026-${invoiceId.slice(0, 8)}`,
        status, date_issued: isoDate(issued), date_due: isoDate(issued + 14),
        notes: '',
        doc_type: 'invoice',
        parent_id: null,
        service_date: '',
        cash_allowance_pct: 0,
        cash_allowance_days: 0,
        discount_pct: 0,
        payment_method: pick(['transfer', 'sepa']),
        sepa_mandate_id: null,
        currency: 'EUR',
        language: 'de',
        project_ref: '',
        external_id: '',
        billing_street: 'Hauptstr. 10',
        billing_zip: c.zip, billing_city: c.city, billing_country: 'DE',
        shipping_street: '',
        shipping_zip: '',
        shipping_city: '',
        shipping_country: 'DE',
      }));

      const posCount = 2 + Math.floor(Math.random() * 5);
      let grossAccumulator = 0;
      for (let p = 0; p < posCount; p++) {
        const prod = pick(productMap);
        const qty = 1000 + Math.floor(Math.random() * 4000);
        grossAccumulator += Math.round(qty * prod.price * (10000 + prod.tax) / 10000000);
        executeOnStream(stream, addPosition({
          id: nextId(), invoice_id: invoiceId, position_nr: (p + 1) * 1000,
          description: prod.name, quantity: qty,
          unit_price: prod.price, tax_rate: prod.tax,
          product_id: prod.id, item_number: prod.sku, unit: prod.unit,
          discount_pct: 0,
          cost_price: prod.cost, position_type: 'product',
        }));
      }

      if (status === 'paid') {
        executeOnStream(stream, createPayment({
          id: nextId(), invoice_id: invoiceId,
          amount: grossAccumulator, paid_at: isoDate(issued + 10),
          method: 'transfer', reference: `TX-${invoiceId}`,
          note: '',
        }));
        executeOnStream(stream, logActivity({
          entity_type: 'invoice', entity_id: invoiceId,
          action: 'payment_received', actor: 'demo', detail: 'voll bezahlt',
        }));
      } else if (status === 'sent' && Math.random() < 0.3) {
        const partial = Math.floor(grossAccumulator * (0.3 + Math.random() * 0.4));
        executeOnStream(stream, createPayment({
          id: nextId(), invoice_id: invoiceId,
          amount: partial, paid_at: isoDate(issued + 5),
          method: 'transfer', reference: `TX-${invoiceId}-1`,
          note: '',
        }));
      }
    }

    if (Math.random() < 0.5) {
      const recId = nextId();
      executeOnStream(stream, createRecurring({
        id: recId, customer_id: customerId,
        template_name: `Wartung ${c.name}`,
        interval_unit: 'month', interval_value: 1,
        next_run: isoDate(15),
        status_template: 'draft',
        notes_template: 'Monatlicher Wartungsvertrag',
      }));
      const maintProd = productMap.find(p => p.sku === 'MAINT-M')!;
      executeOnStream(stream, addRecurringPosition({
        id: nextId(), recurring_id: recId, position_nr: 1000,
        description: maintProd.name, quantity: 1000,
        unit_price: maintProd.price, tax_rate: maintProd.tax,
        unit: maintProd.unit, item_number: maintProd.sku,
        discount_pct: 0,
      }));
    }
  }

  await flushStream(stream);
}

export function SeedMenu() {
  const [busy, setBusy] = useState(false);
  const run = async () => {
    if (busy) return;
    setBusy(true);
    toast.info('Seede Demo-Daten …');
    try {
      await seedSample();
      toast.success('Demo-Daten geseedet');
    } catch (e) {
      toast.error(`Seed fehlgeschlagen: ${(e as Error).message}`);
    } finally {
      setBusy(false);
    }
  };

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="ghost" size="sm" className="w-full justify-start text-muted-foreground">
          <MoreHorizontal className="h-4 w-4" />
          Dev-Werkzeuge
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent side="top" align="start" className="w-56">
        <DropdownMenuLabel>Demo-Daten</DropdownMenuLabel>
        <DropdownMenuSeparator />
        <DropdownMenuItem disabled={busy} onSelect={(e) => { e.preventDefault(); run(); }}>
          <Sprout className="h-4 w-4" />
          Demo-Set seeden
        </DropdownMenuItem>
        <DropdownMenuItem disabled className="text-muted-foreground">
          <Trash2 className="h-4 w-4" />
          Alles löschen (TODO)
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
