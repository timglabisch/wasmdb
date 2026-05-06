import type { Scenario } from '@wasmdb/scenarios';
import { UnmountProbe } from '../../components/UnmountProbe';
import { SEED } from '../../seed';
import { BtnAddMessageR1 } from '../buttons';
import { MSG_LIST_SQL } from '../components/queries';

export const msgUnmountInflight: Scenario = {
  id: 'msg-unmount-inflight',
  category: 'lifecycle',
  title: 'Unmount before write: subscription teardown is safe',
  summary:
    'Subscription teardown safety. Hide the probe → the inner MessageList:R1 unmounts → its subscription closes. Then fire AddMessage(R1). The dead probe instance must not receive any further renders, and the page must not crash.',
  expectations: [
    'Click "Hide R1 (probe)" → the inner list disappears.',
    'Reset render counts.',
    'Click "+ Message in Lobby (R1)" → no crash. The torn-down list stays at zero renders.',
  ],
  shouldStayQuiet: [`MessageList:${SEED.rooms.R1}`],
  subscriptions: [
    { component: 'MessageList:R1 (inside probe)', sql: MSG_LIST_SQL, note: 'On unmount, useQuery tears down its subscription via React\'s effect cleanup. Subsequent dirty-cycles never reach the dead component.' },
  ],
  Body: () => (
    <>
      <section className="panel">
        <h2>Probe</h2>
        <UnmountProbe />
      </section>
      <div className="row">
        <BtnAddMessageR1 />
      </div>
    </>
  ),
};
