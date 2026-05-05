import { MessageCount } from '../components/MessageCount';
import { MessageList } from '../components/MessageList';
import { UnmountProbe } from '../components/UnmountProbe';
import { SEED } from '../seed';
import {
  BtnAddMessageR1,
  BtnAddMessageR1Early,
  BtnBulkAdd20R1,
  BtnBulkDeleteR1,
  BtnDeleteMessage1,
  BtnMoveMessage1ToR2,
} from './buttons';
import type { Scenario } from './types';

const ThreeLists = () => (
  <section className="panel">
    <h2>Messages by room</h2>
    <div className="message-grid">
      <MessageList roomId={SEED.rooms.R1} />
      <MessageList roomId={SEED.rooms.R2} />
      <MessageList roomId={SEED.rooms.R3} />
    </div>
  </section>
);

export const messageScenarios: Scenario[] = [
  {
    id: 'msg-list-membership',
    category: 'messages',
    title: 'AddMessage(R1): only R1 list ticks; existing items stay quiet',
    summary:
      'List-add reactivity: a new row matching the per-room predicate appears. Only MessageList:R1 re-renders. Sibling lists for R2/R3 stay quiet. Existing M1/M2/M3 items did not change → their MessageItem components stay quiet (the new message has its own freshly-mounted item).',
    expectations: [
      'Click "+ Message in Lobby (R1)" → MessageList:R1 ticks; a new item appears.',
      'MessageList:R2, MessageList:R3 stay quiet.',
      'MessageItem:M1, MessageItem:M2, MessageItem:M3 stay quiet.',
    ],
    Body: () => (
      <>
        <ThreeLists />
        <div className="row">
          <BtnAddMessageR1 />
        </div>
      </>
    ),
  },

  {
    id: 'msg-delete',
    category: 'messages',
    title: 'DeleteMessage(M1): R1 list ticks; surviving items quiet',
    summary:
      'List-shrink reactivity: a row leaves the per-room set. Only MessageList:R1 re-renders. Sibling lists stay quiet. Surviving M2/M3 items did not change → their MessageItem components stay quiet.',
    expectations: [
      'Click "Delete first Lobby message (M1)" → MessageList:R1 ticks; M1 disappears.',
      'MessageList:R2, MessageList:R3 stay quiet.',
      'MessageItem:M2, MessageItem:M3 stay quiet.',
    ],
    Body: () => (
      <>
        <ThreeLists />
        <div className="row">
          <BtnDeleteMessage1 />
        </div>
      </>
    ),
  },

  {
    id: 'msg-insert-middle',
    category: 'messages',
    title: 'Insert with earlier created_at: existing items stay quiet',
    summary:
      'Insert with a `created_at` *before* existing rows. The new MessageItem mounts at the head of the list. The existing M1/M2 rows did not change → their MessageItems must NOT re-render. The list itself re-renders (membership change).',
    expectations: [
      'Click "+ Early message (R1)" → MessageList:R1 ticks; a new item appears at the top.',
      'MessageItem:M1, MessageItem:M2 stay quiet.',
    ],
    Body: () => (
      <>
        <section className="panel">
          <h2>R1 messages</h2>
          <MessageList roomId={SEED.rooms.R1} />
        </section>
        <div className="row">
          <BtnAddMessageR1Early />
        </div>
      </>
    ),
  },

  {
    id: 'msg-move',
    category: 'messages',
    title: 'MoveMessage(M1, R1→R2): both lists tick, R3 stays quiet',
    summary:
      'Cross-list membership move. M1 transitions from R1 to R2 by updating its `room_id`. Both lists must observe their own membership change. R3 has nothing to do with this; it stays quiet. The MessageItem:M1 itself re-renders (its `room_id` row changed).',
    expectations: [
      'Click "Move M1: R1 → R2" → MessageList:R1 ticks (loses M1).',
      'MessageList:R2 ticks (gains M1).',
      'MessageList:R3 stays quiet.',
    ],
    Body: () => (
      <>
        <ThreeLists />
        <div className="row">
          <BtnMoveMessage1ToR2 />
        </div>
      </>
    ),
  },

  {
    id: 'msg-bulk-insert',
    category: 'messages',
    title: 'Bulk-insert 20: list renders bounded; siblings + existing rows quiet',
    summary:
      '20 inserts in one tick (synchronous burst from a single click handler). Each command produces optimistic + confirmed dirty cycles → up to ~40 list renders for the affected room. The fence guards against worse-than-linear blow-up. Sibling lists must stay quiet, and existing rows must NOT re-render.',
    expectations: [
      'Click "+ 20 messages (R1)" → MessageList:R1 ticks at most ~40 times.',
      'MessageList:R2, MessageList:R3 stay quiet.',
      'MessageItem:M1, MessageItem:M2 stay quiet.',
    ],
    Body: () => (
      <>
        <ThreeLists />
        <div className="row">
          <BtnBulkAdd20R1 />
        </div>
      </>
    ),
  },

  {
    id: 'msg-bulk-delete',
    category: 'messages',
    title: 'Bulk-insert → bulk-delete: round-trip settles cleanly',
    summary:
      'Insert 20 then delete the same 20. After settling, the list returns to its original state. Sibling rooms never observed any of the noise. Existing seed rows stayed quiet throughout. Test runs the *delete* phase under measurement.',
    expectations: [
      'Click "+ 20 messages (R1)" then reset render counts.',
      'Click "Delete bulk-added messages" → MessageList:R1 ticks.',
      'MessageList:R2, MessageList:R3 stay quiet.',
      'MessageItem:M1, MessageItem:M2 stay quiet.',
    ],
    Body: () => (
      <>
        <ThreeLists />
        <div className="row">
          <BtnBulkAdd20R1 />
          <BtnBulkDeleteR1 />
        </div>
      </>
    ),
  },

  {
    id: 'msg-unmount-inflight',
    category: 'messages',
    title: 'Unmount before write: subscription teardown is safe',
    summary:
      'Subscription teardown safety. Hide the probe → the inner MessageList:R1 unmounts → its subscription closes. Then fire AddMessage(R1). The dead probe instance must not receive any further renders, and the page must not crash.',
    expectations: [
      'Click "Hide R1 (probe)" → the inner list disappears.',
      'Reset render counts.',
      'Click "+ Message in Lobby (R1)" → no crash. The torn-down list stays at zero renders.',
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
  },

  {
    id: 'msg-count',
    category: 'messages',
    title: 'Aggregate COUNT: bounded to its slice',
    summary:
      'Aggregate (`COUNT(messages.id)`) over a per-room slice. The aggregate must react to membership changes in *its* slice only. AddMessage(R1) re-renders MessageCount:R1; MessageCount:R2 and MessageCount:R3 stay quiet.',
    expectations: [
      'Click "+ Message in Lobby (R1)" → MessageCount:R1 ticks; the displayed count grows by 1.',
      'MessageCount:R2, MessageCount:R3 stay quiet.',
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
  },
];
