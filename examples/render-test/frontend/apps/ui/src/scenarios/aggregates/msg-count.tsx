import type { Scenario } from '@wasmdb/scenarios';
import { MessageCount } from '../../components/MessageCount';
import { SEED } from '../../seed';
import { BtnAddMessageR1 } from '../buttons';
import { MSG_COUNT_SQL } from '../components/queries';

export const msgCount: Scenario = {
  id: 'msg-count',
  category: 'aggregates',
  title: 'Aggregate COUNT: bounded to its slice',
  summary:
    'Aggregate (`COUNT(messages.id)`) over a per-room slice. The aggregate must react to membership changes in *its* slice only. AddMessage(R1) re-renders MessageCount:R1; MessageCount:R2 and MessageCount:R3 stay quiet.',
  expectations: [
    'Click "+ Message in Lobby (R1)" → MessageCount:R1 ticks; the displayed count grows by 1.',
    'MessageCount:R2, MessageCount:R3 stay quiet.',
  ],
  shouldRender: [`MessageCount:${SEED.rooms.R1}`],
  shouldStayQuiet: [`MessageCount:${SEED.rooms.R2}`, `MessageCount:${SEED.rooms.R3}`],
  subscriptions: [
    { component: 'MessageCount:*', sql: MSG_COUNT_SQL, note: 'Aggregate over the room\'s message slice. Only re-fires when membership of that slice changes.' },
  ],
  Body: () => (
    <>
      <section className="panel">
        <h2>Per-room counts</h2>
        <div className="row">
          <span>R1: <MessageCount roomId={SEED.rooms.R1} /></span>
          <span>R2: <MessageCount roomId={SEED.rooms.R2} /></span>
          <span>R3: <MessageCount roomId={SEED.rooms.R3} /></span>
        </div>
      </section>
      <div className="row">
        <BtnAddMessageR1 />
      </div>
    </>
  ),
};
