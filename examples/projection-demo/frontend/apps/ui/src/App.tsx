import { useEffect, useState } from 'react';
import { useQuery, useWasm } from '@wasmdb/client';
import { DebugToolbar } from '@wasmdb/debug-toolbar';
import { post } from './commands';
import { seed } from './seed';
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

interface LedgerRow {
  account: string;
  seq: number;
  committed: boolean;
  amountCents: number;
}

function useLedger(): LedgerRow[] {
  return useQuery<LedgerRow>(
    'SELECT REACTIVE(ledger_log.command_id), ledger_log.account, ledger_log.seq,' +
      ' ledger_log.committed, ledger_log.payload' +
      ' FROM ledger_log ORDER BY ledger_log.account, ledger_log.seq',
    // col 0 is the REACTIVE(...) marker that binds the subscription; skip it.
    ([, account, seq, committed, payload]) => ({
      account: account as string,
      seq: seq as number,
      committed: (committed as number) !== 0,
      // The payload IS the event: the RPC form of the PostEntry command.
      amountCents: (JSON.parse(payload as string) as { amount_cents: number }).amount_cents,
    }),
  );
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
              <th className="num">seq</th>
              <th className="num">amount</th>
              <th>state</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((e) => (
              <tr key={`${e.account}-${e.seq}`}>
                <td className="acct">{e.account}</td>
                <td className="num muted">{e.seq}</td>
                <td className={`num amount ${e.amountCents < 0 ? 'neg' : 'pos'}`}>
                  {euro(e.amountCents)}
                </td>
                <td>
                  <span className={`badge ${e.committed ? 'committed' : 'pending'}`}>
                    {e.committed ? 'committed' : 'pending'}
                  </span>
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

function Controls() {
  const [account, setAccount] = useState('alice');
  const [amount, setAmount] = useState('10.00');

  const cents = Math.round(parseFloat(amount || '0') * 100);
  const valid = Number.isFinite(cents) && cents > 0 && account.trim().length > 0;

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
    </section>
  );
}

// ── App ──────────────────────────────────────────────────────────────

// The reactive body is its own component so its `useQuery` hooks first mount
// *after* wasm is ready — a subscription created before the boot completes
// never binds (and never retries).
function Dashboard() {
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
      {import.meta.env.DEV && <DebugToolbar />}
    </main>
  );
}

export default function App() {
  const ready = useWasm();
  const [seeded, setSeeded] = useState(false);

  useEffect(() => {
    if (!ready || seeded) return;
    void seed().then(() => setSeeded(true));
  }, [ready, seeded]);

  if (!ready) return <div className="loading">loading wasm…</div>;

  return <Dashboard />;
}
