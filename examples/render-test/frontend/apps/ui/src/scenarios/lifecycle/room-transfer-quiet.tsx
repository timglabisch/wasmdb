import type { Scenario } from '@wasmdb/scenarios';
import { MessageList } from '../../components/MessageList';
import { RoomList } from '../../components/RoomList';
import { SEED } from '../../seed';
import { BtnTransferRoom1ToB } from '../buttons';

export const roomTransferQuiet: Scenario = {
  id: 'room-transfer-quiet',
  category: 'lifecycle',
  title: 'Transfer room: subscription teardown — old owner badge stays quiet elsewhere',
  summary:
    'After TransferRoom(R1, B), the UserBadge:Alice@room:R1 instance unmounts and is replaced by a fresh UserBadge:Bob@room:R1. The old A-badge subscription is torn down. Alice\'s *other* instances (R3 owner, M1 author) must stay quiet — her row didn\'t change.',
  expectations: [
    'Click "Transfer Lobby (R1) → Bob".',
    'A new UserBadge:Bob@room:R1 mounts and renders.',
    'UserBadge:Alice@room:R3 stays quiet.',
    'UserBadge:Alice@msg:M1 stays quiet.',
  ],
  shouldRender: [`UserBadge:${SEED.users.B}@room:${SEED.rooms.R1}`],
  shouldStayQuiet: [
    `UserBadge:${SEED.users.A}@room:${SEED.rooms.R3}`,
    `UserBadge:${SEED.users.A}@msg:${SEED.messages.M1}`,
  ],
  subscriptions: [
    {
      component: 'UserBadge:* (per-instance)',
      sql: `SELECT users.id, users.name, users.status
FROM users
WHERE REACTIVE(users.id = UUID '<user-id>')`,
      note: 'When the parent passes a different user-id prop, the badge unmounts and remounts. Alice\'s row was never touched, so her remaining badges stay quiet.',
    },
  ],
  Body: () => (
    <>
      <RoomList />
      <section className="panel">
        <h2>R1 messages (M1 author = Alice)</h2>
        <MessageList roomId={SEED.rooms.R1} />
      </section>
      <div className="row">
        <BtnTransferRoom1ToB />
      </div>
    </>
  ),
};
