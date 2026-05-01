import { memo, useCallback, useRef, useState } from 'react';
import { X } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow, TableFooter,
} from '@/components/ui/table';
import { Button } from '@/components/ui/button';
import { BlurInput, BlurNumberInput } from '@/components/form';
import { execute, useQuery, nextId } from '@/wasm';
import { addPosition } from '@/commands/position/addPosition';
import { updatePosition } from '@/commands/position/updatePosition';
import { deletePosition } from '@/commands/position/deletePosition';
import { formatEuro } from '@/shared/lib/format';
import {
  useInvoicePositions, computeGrossCents, computeNetCents,
} from '@/shared/lib/gross';
import { AddPositionControls } from '@/features/invoice/components/AddPositionControls';

/** Subtle grid inputs — no visible border until focus, so the table reads like a grid. */
const GRID_INPUT =
  'h-7 border-transparent bg-transparent shadow-none focus-visible:border-input focus-visible:bg-background';

interface PosIdRow { id: string; position_nr: number }

/**
 * Positions grid. The list subscribes only to ids + position_nr for ordering.
 * Each row subscribes to its own column set. New rows are added inline
 * (no modal) via `AddPositionControls`; the new row's description input is
 * autofocused so typing can start immediately. Pressing Enter on the last
 * row's description appends another empty row, Fastbill-style.
 */
export function PositionsCard({ invoiceId }: { invoiceId: string }) {
  const ids = useQuery<PosIdRow>(
    `SELECT positions.id, positions.position_nr FROM positions ` +
    `WHERE REACTIVE(positions.invoice_id = UUID '${invoiceId}') ORDER BY positions.position_nr`,
    ([id, nr]) => ({ id: id as string, position_nr: nr as number }),
  );
  const nextPositionNr = (ids.length > 0 ? ids[ids.length - 1].position_nr : 0) + 1;

  const [autoFocusId, setAutoFocusId] = useState<string | null>(null);

  const positions = useInvoicePositions(invoiceId);
  const net = computeNetCents(positions);
  const gross = computeGrossCents(positions);
  const vat = gross - net;

  // Shared handler for "add another empty row" used by both the bottom
  // button and Enter-on-last-description. Uses the current next nr based on
  // the latest ids snapshot.
  const nextNrRef = useRef(nextPositionNr);
  nextNrRef.current = nextPositionNr;
  const lastIdRef = useRef<string>(ids.length > 0 ? ids[ids.length - 1].id : '');
  lastIdRef.current = ids.length > 0 ? ids[ids.length - 1].id : '';

  const addEmptyRow = useCallback(() => {
    const id = nextId();
    execute(addPosition({
      id, invoice_id: invoiceId, position_nr: nextNrRef.current,
    }));
    setAutoFocusId(id);
  }, [invoiceId]);

  const onLastRowEnter = useCallback(() => {
    addEmptyRow();
  }, [addEmptyRow]);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Positionen</CardTitle>
      </CardHeader>
      <CardContent className="px-0 pb-3">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-10 pl-4">#</TableHead>
              <TableHead className="w-24">Art</TableHead>
              <TableHead>Beschreibung</TableHead>
              <TableHead className="w-20 text-right">Menge</TableHead>
              <TableHead className="w-16">Einheit</TableHead>
              <TableHead className="w-24 text-right">Einzelpreis</TableHead>
              <TableHead className="w-16 text-right">MwSt</TableHead>
              <TableHead className="w-24 text-right">Zeilensumme</TableHead>
              <TableHead className="w-8 pr-4" />
            </TableRow>
          </TableHeader>
          <TableBody>
            {ids.length === 0 ? (
              <TableRow>
                <TableCell colSpan={9} className="py-6 text-center text-xs text-muted-foreground">
                  Noch keine Positionen — füge unten deine erste Zeile hinzu.
                </TableCell>
              </TableRow>
            ) : (
              ids.map((r, idx) => {
                const isLast = idx === ids.length - 1;
                return (
                  <PositionRow
                    key={r.id}
                    positionId={r.id}
                    displayNr={idx + 1}
                    autoFocus={autoFocusId === r.id}
                    onEnterIfLast={isLast ? onLastRowEnter : undefined}
                  />
                );
              })
            )}
            <TableRow className="border-t border-dashed hover:bg-transparent">
              <TableCell colSpan={9} className="py-1.5 pl-4">
                <AddPositionControls
                  invoiceId={invoiceId}
                  nextPositionNr={nextPositionNr}
                  onAdded={setAutoFocusId}
                />
              </TableCell>
            </TableRow>
          </TableBody>
          <TableFooter>
            <TableRow className="hover:bg-transparent">
              <TableCell colSpan={7} className="pl-4 text-right text-[11px] uppercase tracking-wider text-muted-foreground">
                Netto
              </TableCell>
              <TableCell className="text-right tabular-nums">{formatEuro(net)}</TableCell>
              <TableCell />
            </TableRow>
            <TableRow className="hover:bg-transparent">
              <TableCell colSpan={7} className="pl-4 text-right text-[11px] uppercase tracking-wider text-muted-foreground">
                MwSt
              </TableCell>
              <TableCell className="text-right tabular-nums">{formatEuro(vat)}</TableCell>
              <TableCell />
            </TableRow>
            <TableRow className="hover:bg-transparent">
              <TableCell colSpan={7} className="pl-4 text-right text-[13px] font-semibold">
                Brutto
              </TableCell>
              <TableCell className="text-right text-[13px] font-semibold tabular-nums">
                {formatEuro(gross)}
              </TableCell>
              <TableCell />
            </TableRow>
          </TableFooter>
        </Table>
      </CardContent>
    </Card>
  );
}

interface PosRowFull {
  description: string;
  quantity: number;
  unit_price: number;
  tax_rate: number;
  product_id: string | null;
  item_number: string;
  unit: string;
  discount_pct: number;
  cost_price: number;
  position_type: string;
}

const PositionRow = memo(function PositionRow({
  positionId,
  displayNr,
  autoFocus,
  onEnterIfLast,
}: {
  positionId: string;
  displayNr: number;
  autoFocus: boolean;
  onEnterIfLast?: () => void;
}) {
  const rows = useQuery<PosRowFull>(
    `SELECT positions.description, positions.quantity, positions.unit_price, positions.tax_rate, ` +
    `positions.product_id, positions.item_number, positions.unit, positions.discount_pct, ` +
    `positions.cost_price, positions.position_type ` +
    `FROM positions WHERE REACTIVE(positions.id = UUID '${positionId}')`,
    ([desc, qty, up, tr, pid, item, unit, disc, cost, pt]) => ({
      description: desc as string,
      quantity: qty as number,
      unit_price: up as number,
      tax_rate: tr as number,
      product_id: (pid as string | null) ?? null,
      item_number: item as string,
      unit: unit as string,
      discount_pct: disc as number,
      cost_price: cost as number,
      position_type: pt as string,
    }),
  );

  const patch = useCallback((partial: Partial<PosRowFull>) => {
    const cur = rows[0];
    if (!cur) return;
    execute(updatePosition({
      id: positionId,
      description: cur.description,
      quantity: cur.quantity,
      unit_price: cur.unit_price,
      tax_rate: cur.tax_rate,
      product_id: cur.product_id,
      item_number: cur.item_number,
      unit: cur.unit,
      discount_pct: cur.discount_pct,
      cost_price: cur.cost_price,
      position_type: cur.position_type,
      ...partial,
    }));
  }, [positionId, rows]);

  const onDelete = useCallback(() => {
    execute(deletePosition({ id: positionId }));
  }, [positionId]);

  const onDescriptionKeyDown = useCallback((e: React.KeyboardEvent<HTMLInputElement>) => {
    // Enter on the last row's description appends a new empty row + focuses it.
    // The BlurInput default still commits-on-Enter (via its own handler) — we
    // fire the append *before* letting it blur so the new row is already
    // queued and can be autofocused on render.
    if (e.key === 'Enter' && onEnterIfLast) {
      onEnterIfLast();
    }
  }, [onEnterIfLast]);

  const p = rows[0];
  if (!p) return null;

  const raw = (p.quantity * p.unit_price) / 1000;
  const netLine = Math.round(raw * (10000 - p.discount_pct) / 10000);

  return (
    <TableRow className="hover:bg-muted/30">
      <TableCell className="pl-4 text-[11px] text-muted-foreground tabular-nums">{displayNr}</TableCell>
      <TableCell className="text-[11px] text-muted-foreground">
        {p.position_type === 'service'
          ? 'Dienst'
          : p.position_type === 'product'
          ? 'Produkt'
          : p.position_type}
      </TableCell>
      <TableCell>
        <BlurInput
          className={GRID_INPUT}
          value={p.description}
          onCommit={(next) => patch({ description: next })}
          placeholder="Beschreibung"
          autoFocus={autoFocus}
          onKeyDown={onDescriptionKeyDown}
        />
      </TableCell>
      <TableCell className="text-right">
        <BlurNumberInput
          className={`${GRID_INPUT} text-right tabular-nums`}
          value={p.quantity}
          onCommit={(next) => patch({ quantity: next })}
          step={1}
        />
      </TableCell>
      <TableCell>
        <BlurInput
          className={GRID_INPUT}
          value={p.unit}
          onCommit={(next) => patch({ unit: next })}
        />
      </TableCell>
      <TableCell className="text-right">
        <BlurNumberInput
          className={`${GRID_INPUT} text-right tabular-nums`}
          value={p.unit_price}
          onCommit={(next) => patch({ unit_price: next })}
          step={1}
        />
      </TableCell>
      <TableCell className="text-right">
        <BlurNumberInput
          className={`${GRID_INPUT} text-right tabular-nums`}
          value={p.tax_rate}
          onCommit={(next) => patch({ tax_rate: next })}
          step={1}
        />
      </TableCell>
      <TableCell className="text-right tabular-nums">{formatEuro(netLine)}</TableCell>
      <TableCell className="pr-4">
        <Button
          variant="ghost"
          size="icon"
          className="h-6 w-6 text-muted-foreground hover:text-destructive"
          onClick={onDelete}
        >
          <X className="h-3.5 w-3.5" />
        </Button>
      </TableCell>
    </TableRow>
  );
});
