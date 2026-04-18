/**
 * Full-row shapes for customer-related tables. Mirrors the write-command
 * payloads (UpdateCustomer, UpdateContact, UpdateSepaMandate) so the patch
 * hooks can compose the required full-row payloads at write time.
 */
export interface CustomerRow {
  name: string; email: string;
  company_type: string; tax_id: string; vat_id: string;
  payment_terms_days: number; default_discount_pct: number;
  billing_street: string; billing_zip: string; billing_city: string; billing_country: string;
  shipping_street: string; shipping_zip: string; shipping_city: string; shipping_country: string;
  default_iban: string; default_bic: string; notes: string;
}

export interface ContactRow {
  name: string; email: string; phone: string; role: string; is_primary: number;
}

export interface SepaMandateRow {
  mandate_ref: string; iban: string; bic: string;
  holder_name: string; signed_at: string; status: string;
}
