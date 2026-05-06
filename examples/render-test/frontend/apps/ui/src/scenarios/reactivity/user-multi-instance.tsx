import type { Scenario } from '@wasmdb/scenarios';
import { MessageList } from '../../components/MessageList';
import { RoomList } from '../../components/RoomList';
import { SEED } from '../../seed';
import { BtnRenameUserA } from '../buttons';
import { USER_BADGE_SQL } from '../components/queries';

export const userMultiInstance: Scenario = {
  id: 'user-multi-instance',
  category: 'reactivity',
  title: 'Same user, multiple instances: every UserBadge:A ticks',
  summary:
    'Alice appears in three places — owner badge of R1, owner badge of R3, author badge of M1. UpdateUserName(A) must fan out to all three contexts, regardless of context tag.',
  expectations: [
    'Click "Rename Alice" → all three Alice badges tick:',
    '— UserBadge:Alice@room:R1 (owner)',
    '— UserBadge:Alice@room:R3 (owner)',
    '— UserBadge:Alice@msg:M1 (author)',
    'Bob/Carol badges stay quiet.',
  ],
  shouldRender: [`*UserBadge:${SEED.users.A}*`],
  shouldStayQuiet: [`*UserBadge:${SEED.users.B}*`, `*UserBadge:${SEED.users.C}*`],
  subscriptions: [
    {
      component: 'UserBadge:* (× many instances)',
      sql: USER_BADGE_SQL,
      note: 'Each <UserBadge> instance subscribes independently — the engine fans out to all instances bound to the same user-id.',
    },
  ],
  Body: () => (
    <>
      <RoomList />
      <section className="panel">
        <h2>R1 messages (M1 is by Alice)</h2>
        <MessageList roomId={SEED.rooms.R1} />
      </section>
      <div className="row">
        <BtnRenameUserA />
      </div>
    </>
  ),
};
