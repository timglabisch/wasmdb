import { useEffect, useState } from 'react';
import { bootstrap, useQuery, useWasm } from '@wasmdb/client';
import { DebugToolbar } from '@wasmdb/debug-toolbar';
import {
  activateAccountActivity,
  deactivateAccountActivity,
  post,
  foreignWriteCarol,
} from './commands';
import './index.css';

// ── Derived read model: `balance`, maintained by BalanceFold ─────────

interface BalanceRow {
  account: string;
  balanceCents: number;
  entries: number;
}

function useBalances(): BalanceRow[] {
  return useQuery<BalanceRow>(
    'SELECT REACTIVE(balance.account), balance.account, balance.balance_cents, balance.entries' +
      ' FROM balance ORDER BY balance.account',
    // col 0 is the REACTIVE(...) marker that binds the subscription; skip it.
    ([, account, balanceCents, entries]) => ({
      account: account as string,
      balanceCents: balanceCents as number,
      entries: entries as number,
    }),
  );
}

// ── The raw event log: `ledger_log` ──────────────────────────────────

// The chain root sentinel (design §11): the nil UUID, mirroring
// `tables::ROOT_PARENT`.
const ROOT_PARENT = '00000000-0000-0000-0000-000000000000';

interface LedgerRow {
  commandId: string;
  account: string;
  clientParentId: string;
  // null while pending (off-chain); the server's link once committed.
  serverParentId: string | null;
  amountCents: number;
}

// Committed once the server has linked the row into the chain (§11).
const isCommitted = (e: LedgerRow): boolean => e.serverParentId !== null;
// Drift: the server sorted the row after a different predecessor than the
// client optimistically assumed (§11.4). Only meaningful once committed.
const hasDrift = (e: LedgerRow): boolean =>
  isCommitted(e) && e.serverParentId !== e.clientParentId;

// Fold order within one account — mirrors `ProjectionLog::in_fold_order`
// (§11.3): the committed server-parent chain from ROOT, then the pending
// client-parent tail. There is no `seq` column any more; order IS the
// chain. Rows a broken chain can't reach are appended last.
function chainOrder(rows: LedgerRow[]): LedgerRow[] {
  const committedByParent = new Map<string, LedgerRow>();
  const pendingByParent = new Map<string, LedgerRow>();
  for (const r of rows) {
    if (r.serverParentId !== null) committedByParent.set(r.serverParentId, r);
    else pendingByParent.set(r.clientParentId, r);
  }
  const out: LedgerRow[] = [];
  const seen = new Set<string>();
  let cursor = ROOT_PARENT;
  while (committedByParent.has(cursor)) {
    const r = committedByParent.get(cursor)!;
    if (seen.has(r.commandId)) break;
    seen.add(r.commandId);
    out.push(r);
    cursor = r.commandId;
  }
  while (pendingByParent.has(cursor)) {
    const r = pendingByParent.get(cursor)!;
    if (seen.has(r.commandId)) break;
    seen.add(r.commandId);
    out.push(r);
    cursor = r.commandId;
  }
  for (const r of rows) if (!seen.has(r.commandId)) out.push(r);
  return out;
}

function useLedger(): LedgerRow[] {
  const rows = useQuery<LedgerRow>(
    'SELECT REACTIVE(ledger_log.command_id), ledger_log.command_id, ledger_log.account,' +
      ' ledger_log.client_parent_id, ledger_log.server_parent_id, ledger_log.payload' +
      ' FROM ledger_log ORDER BY ledger_log.account',
    // col 0 is the REACTIVE(...) marker that binds the subscription; skip it.
    ([, commandId, account, clientParentId, serverParentId, payload]) => ({
      commandId: commandId as string,
      account: account as string,
      clientParentId: clientParentId as string,
      serverParentId: (serverParentId as string | null) ?? null,
      // The payload is the EntryPosted event: { amount_cents }.
      amountCents: (JSON.parse(payload as string) as { amount_cents: number }).amount_cents,
    }),
  );

  // Reconstruct each account's chain order client-side — SQL can't traverse
  // the parent links, and `seq` is gone (§11).
  const byAccount = new Map<string, LedgerRow[]>();
  for (const r of rows) {
    const group = byAccount.get(r.account) ?? [];
    group.push(r);
    byAccount.set(r.account, group);
  }
  return [...byAccount.keys()].sort().flatMap((a) => chainOrder(byAccount.get(a)!));
}

// ── Formatting ───────────────────────────────────────────────────────

const euro = (cents: number): string =>
  `${cents < 0 ? '−' : ''}€${(Math.abs(cents) / 100).toFixed(2)}`;

// ── Panels ───────────────────────────────────────────────────────────

function Balances({ rows }: { rows: BalanceRow[] }) {
  return (
    <section className="panel">
      <h2>
        Balances <small>derived — table <code>balance</code>, maintained by BalanceFold</small>
      </h2>
      {rows.length === 0 ? (
        <p className="empty">no accounts yet</p>
      ) : (
        <table className="grid">
          <thead>
            <tr>
              <th>account</th>
              <th className="num">balance</th>
              <th className="num">entries</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((b) => (
              <tr key={b.account}>
                <td className="acct">{b.account}</td>
                <td className={`num amount ${b.balanceCents < 0 ? 'neg' : 'pos'}`}>
                  {euro(b.balanceCents)}
                </td>
                <td className="num muted">{b.entries}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}

function Ledger({ rows }: { rows: LedgerRow[] }) {
  return (
    <section className="panel">
      <h2>
        Event log <small>append-only — table <code>ledger_log</code>, one row per posted entry</small>
      </h2>
      {rows.length === 0 ? (
        <p className="empty">no events yet</p>
      ) : (
        <table className="grid">
          <thead>
            <tr>
              <th>account</th>
              <th className="num">amount</th>
              <th>state</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((e) => (
              <tr key={e.commandId}>
                <td className="acct">{e.account}</td>
                <td className={`num amount ${e.amountCents < 0 ? 'neg' : 'pos'}`}>
                  {euro(e.amountCents)}
                </td>
                <td>
                  <span className={`badge ${isCommitted(e) ? 'committed' : 'pending'}`}>
                    {isCommitted(e) ? 'committed' : 'pending'}
                  </span>
                  {hasDrift(e) && (
                    <span className="badge drift" title="server linked it after a different row than the client assumed">
                      drift
                    </span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}

const ACCOUNTS = ['alice', 'bob', 'carol'];

// ── Demand projection: `account_activity`, activated per account (§12) ─

interface ActivityRow {
  account: string;
  deposits: number;
  withdrawals: number;
  largestCents: number;
}

// The toggles survive F5 in localStorage; the wasm-side instances do NOT
// (client memory) — the panel re-activates the stored accounts on mount
// once the bootstrap has run.
const ACTIVITY_STORAGE_KEY = 'projection-demo.activity-active';

function loadActiveAccounts(): string[] {
  try {
    const parsed = JSON.parse(localStorage.getItem(ACTIVITY_STORAGE_KEY) ?? '[]') as unknown;
    return Array.isArray(parsed) ? parsed.filter((a): a is string => typeof a === 'string') : [];
  } catch {
    return [];
  }
}

/** One activated instance: holds the activation for its lifetime and
 *  renders the materialized `account_activity` row reactively. */
function ActivityInstance({ account, booted }: { account: string; booted: boolean }) {
  // Activate after the bootstrap so the initial fold sees the pulled
  // history; deactivate (refcounted, retracts the row) on unmount/toggle.
  useEffect(() => {
    if (!booted) return;
    activateAccountActivity(account);
    return () => deactivateAccountActivity(account);
  }, [account, booted]);

  const rows = useQuery<ActivityRow>(
    'SELECT REACTIVE(account_activity.account), account_activity.account,' +
      ' account_activity.deposits, account_activity.withdrawals, account_activity.largest_cents' +
      ' FROM account_activity WHERE account_activity.account = :account',
    // col 0 is the REACTIVE(...) marker that binds the subscription; skip it.
    ([, acct, deposits, withdrawals, largestCents]) => ({
      account: acct as string,
      deposits: deposits as number,
      withdrawals: withdrawals as number,
      largestCents: largestCents as number,
    }),
    { account },
  );
  const row = rows[0];

  return (
    <div className="activity-card">
      <span className="acct">{account}</span>
      {row ? (
        <>
          <span className="muted">
            {row.deposits} deposit{row.deposits === 1 ? '' : 's'} ·{' '}
            {row.withdrawals} withdrawal{row.withdrawals === 1 ? '' : 's'}
          </span>
          <span className={`amount ${row.largestCents < 0 ? 'neg' : 'pos'}`}>
            largest {euro(row.largestCents)}
          </span>
        </>
      ) : (
        <span className="muted">materializing…</span>
      )}
    </div>
  );
}

function AccountDetail({ booted }: { booted: boolean }) {
  const [active, setActive] = useState<string[]>(loadActiveAccounts);

  useEffect(() => {
    localStorage.setItem(ACTIVITY_STORAGE_KEY, JSON.stringify(active));
  }, [active]);

  const toggle = (account: string) =>
    setActive((prev) =>
      prev.includes(account) ? prev.filter((a) => a !== account) : [...prev, account],
    );

  return (
    <section className="panel">
      <h2>
        Account detail{' '}
        <small>
          demand projection — table <code>account_activity</code>, one activated{' '}
          <strong>ActivityFold</strong> instance per toggle
        </small>
      </h2>
      <p className="hint">
        Unlike <code>balance</code> (data presence: every account with local rows is
        materialized), this table only holds accounts whose instance is <em>activated</em> —
        the 10k-entities case in miniature. Toggle an account to materialize it on demand;
        toggling off retracts its row.
      </p>
      <div className="row quick">
        {ACCOUNTS.map((a) => (
          <button
            key={a}
            className={`chip ${active.includes(a) ? 'active' : ''}`}
            onClick={() => toggle(a)}
          >
            {active.includes(a) ? '◉' : '○'} {a}
          </button>
        ))}
      </div>
      {active.length === 0 ? (
        <p className="empty">no instance activated — nothing is materialized</p>
      ) : (
        active.map((a) => <ActivityInstance key={a} account={a} booted={booted} />)
      )}
    </section>
  );
}

function Controls() {
  const [account, setAccount] = useState('alice');
  const [amount, setAmount] = useState('10.00');
  const [repairMsg, setRepairMsg] = useState<string | null>(null);
  const [foreignBusy, setForeignBusy] = useState(false);

  const cents = Math.round(parseFloat(amount || '0') * 100);
  const valid = Number.isFinite(cents) && cents > 0 && account.trim().length > 0;

  // Advance carol out-of-band, then let bootstrap gap-repair the new rows
  // in — a live §11.4 demonstration mid-session.
  const runForeignWrite = () => {
    setForeignBusy(true);
    setRepairMsg(null);
    void foreignWriteCarol()
      .then((n) =>
        setRepairMsg(`another writer advanced carol · repair pulled ${n} row${n === 1 ? '' : 's'} in`),
      )
      .catch((e) => setRepairMsg(`foreign write failed: ${String(e)}`))
      .finally(() => setForeignBusy(false));
  };

  return (
    <section className="panel controls">
      <h2>
        Post an entry <small>fires a <code>PostEntry</code> command → appends to the log</small>
      </h2>
      <div className="row">
        <label>
          account&nbsp;
          <input
            value={account}
            onChange={(e) => setAccount(e.target.value)}
            list="accounts"
            spellCheck={false}
          />
          <datalist id="accounts">
            {ACCOUNTS.map((a) => (
              <option key={a} value={a} />
            ))}
          </datalist>
        </label>
        <label>
          amount €&nbsp;
          <input
            type="number"
            min="0"
            step="0.01"
            value={amount}
            onChange={(e) => setAmount(e.target.value)}
          />
        </label>
        <button
          className="deposit"
          disabled={!valid}
          onClick={() => post(account.trim(), cents)}
        >
          + Deposit
        </button>
        <button
          className="withdraw"
          disabled={!valid}
          onClick={() => post(account.trim(), -cents)}
        >
          − Withdraw
        </button>
      </div>
      <div className="row quick">
        {ACCOUNTS.map((a) => (
          <button key={a} className="chip" onClick={() => setAccount(a)}>
            {a}
          </button>
        ))}
      </div>
      <div className="row foreign">
        <button className="chip foreign" disabled={foreignBusy} onClick={runForeignWrite}>
          {foreignBusy ? 'syncing…' : 'simulate another writer → carol'}
        </button>
        {repairMsg && <span className="repair-status">{repairMsg}</span>}
      </div>
    </section>
  );
}

// ── App ──────────────────────────────────────────────────────────────

// The reactive body is its own component so its `useQuery` hooks first mount
// *after* wasm is ready — a subscription created before the boot completes
// never binds (and never retries).
function Dashboard({ booted }: { booted: boolean }) {
  const balances = useBalances();
  const ledger = useLedger();

  return (
    <main className="app">
      <header>
        <h1>wasmdb · projection-demo</h1>
        <p>
          An event-sourced account ledger. Every <code>PostEntry</code> appends a row to the
          <code> ledger_log</code>; the <strong>BalanceFold</strong> projection folds each
          account's rows into the derived <code>balance</code> table — incrementally, at the
          notify chokepoint. Post an entry and watch both update live.
        </p>
      </header>
      <Controls />
      <div className="cols">
        <Balances rows={balances} />
        <Ledger rows={ledger} />
      </div>
      <AccountDetail booted={booted} />
      {import.meta.env.DEV && <DebugToolbar />}
    </main>
  );
}

export default function App() {
  const ready = useWasm();
  const [booted, setBooted] = useState(false);

  // The client DB is wasm-memory only, so every load starts empty. Rather
  // than re-seed hardcoded rows, rebuild state from the server: fetch the
  // chain heads and walk each chain to ROOT (design §11.4). This is itself
  // a gap-repair — and why a reload no longer wipes the ledger.
  useEffect(() => {
    if (!ready || booted) return;
    bootstrap('ledger_log')
      .catch((e) => console.error('[bootstrap] failed — is the confirm-server up on :3126?', e))
      .finally(() => setBooted(true));
  }, [ready, booted]);

  if (!ready) return <div className="loading">loading wasm…</div>;

  return <Dashboard booted={booted} />;
}
