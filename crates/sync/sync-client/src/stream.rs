use std::collections::VecDeque;
use database::Database;
use sync::command::{Command, CommandError};
use sync::protocol::{CommandRequest, CommandResponse, SeqNo, StreamId, Verdict};
use sync::zset::ZSet;

#[derive(Debug, Clone)]
struct PendingEntry<C: Command> {
    seq_no: SeqNo,
    command: C,
    zset: ZSet,
}

pub struct Stream<C: Command> {
    pub id: StreamId,
    next_seq_no: u64,
    pending: VecDeque<PendingEntry<C>>,
    confirmed_buffer: Vec<CommandResponse>,
}

/// What the caller should do after processing a server response.
#[derive(Debug)]
pub enum StreamAction {
    /// Stream is empty.
    Idle,
    /// Waiting for more responses before we can apply.
    WaitingForResponse,
    /// All pending commands are confirmed. To reconcile the single database,
    /// the caller inverts each `client_zsets` entry (rollback of the
    /// optimistic delta) and applies each `confirmed_zsets` entry (the
    /// canonical server result). Both vectors are in original seq order.
    AllConfirmed {
        client_zsets: Vec<ZSet>,
        confirmed_zsets: Vec<ZSet>,
    },
    /// The stream was rejected. All optimistic ZSets must be rolled back.
    Rejected {
        reason: String,
        discarded_zsets: Vec<ZSet>,
    },
}

impl<C: Command> Stream<C> {
    pub fn new(id: StreamId) -> Self {
        Self {
            id,
            next_seq_no: 0,
            pending: VecDeque::new(),
            confirmed_buffer: Vec::new(),
        }
    }

    /// Execute a command optimistically on the local database.
    /// Returns the CommandRequest to send to the server.
    pub fn push_command(
        &mut self,
        command: C,
        db: &mut Database,
    ) -> Result<CommandRequest<C>, CommandError> {
        let seq_no = SeqNo(self.next_seq_no);
        self.next_seq_no += 1;

        let zset = command.execute_optimistic(db)?;

        let request = CommandRequest {
            stream_id: self.id,
            seq_no,
            command: command.clone(),
            client_zset: zset.clone(),
        };

        self.pending.push_back(PendingEntry {
            seq_no,
            command,
            zset,
        });

        Ok(request)
    }

    /// Receive a server response. Returns what the caller should do.
    pub fn receive_response(&mut self, response: CommandResponse) -> StreamAction {
        self.confirmed_buffer.push(response);
        self.try_flush()
    }

    /// Try to process buffered responses in order.
    /// Only returns AllConfirmed when ALL pending entries are confirmed.
    fn try_flush(&mut self) -> StreamAction {
        if self.pending.is_empty() {
            return StreamAction::Idle;
        }

        // Sort buffer by seq_no for ordered processing
        self.confirmed_buffer.sort_by_key(|r| r.seq_no);

        // Scan pending entries in order.
        // - If we hit a reject: immediately reject the entire stream.
        // - If we hit a missing response: wait.
        // - If all are confirmed: flush.
        let mut confirmed_zsets = Vec::with_capacity(self.pending.len());

        for entry in &self.pending {
            let pos = self.confirmed_buffer.iter()
                .position(|r| r.seq_no == entry.seq_no);

            match pos {
                Some(i) => {
                    match &self.confirmed_buffer[i].verdict {
                        Verdict::Confirmed { server_zset } => {
                            confirmed_zsets.push(server_zset.clone());
                        }
                        Verdict::Rejected { reason } => {
                            let reason = reason.clone();
                            let discarded = self.pending.drain(..)
                                .map(|e| e.zset)
                                .collect();
                            self.confirmed_buffer.clear();
                            return StreamAction::Rejected {
                                reason,
                                discarded_zsets: discarded,
                            };
                        }
                    }
                }
                None => return StreamAction::WaitingForResponse,
            }
        }

        // All pending confirmed — surface both client (optimistic) and
        // server (canonical) ZSets so the caller can roll back + reapply.
        let client_zsets: Vec<ZSet> = self.pending.drain(..).map(|e| e.zset).collect();
        self.confirmed_buffer.clear();

        StreamAction::AllConfirmed { client_zsets, confirmed_zsets }
    }

    /// Debug info: (seq_no, zset_entry_count) per pending entry.
    pub fn pending_detail(&self) -> Vec<(u64, usize)> {
        self.pending.iter().map(|e| (e.seq_no.0, e.zset.len())).collect()
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    pub fn is_idle(&self) -> bool {
        self.pending.is_empty()
    }

    /// Get all pending commands for replay after a rebuild.
    pub fn pending_commands(&self) -> Vec<C> {
        self.pending.iter().map(|e| e.command.clone()).collect()
    }
}
