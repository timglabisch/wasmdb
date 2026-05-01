use std::collections::HashMap;
use database::Database;
use database_reactive::ReactiveDatabase;
use sync::command::{Command, CommandError};
use sync::protocol::{CommandRequest, CommandResponse, StreamId};
use sync::zset::ZSet;
use crate::stream::{Stream, StreamAction};

pub struct SyncClient<C: Command> {
    /// Single source of truth: confirmed state + fetcher-loaded data + pending
    /// optimistic deltas. Reads (`db()`/`db_mut()`) and reactive subscriptions
    /// run against this. On confirm we invert the optimistic delta and apply
    /// the server's canonical ZSet; on reject we invert and drop.
    db: ReactiveDatabase,
    streams: HashMap<StreamId, Stream<C>>,
    next_stream_id: u64,
}

#[derive(Debug)]
pub enum SyncClientError {
    UnknownStream(StreamId),
    CommandError(CommandError),
}

impl std::fmt::Display for SyncClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncClientError::UnknownStream(id) => write!(f, "unknown stream: {:?}", id),
            SyncClientError::CommandError(e) => write!(f, "command error: {e}"),
        }
    }
}

impl std::error::Error for SyncClientError {}

impl<C: Command> SyncClient<C> {
    pub fn new(db: Database) -> Self {
        Self {
            db: ReactiveDatabase::from_database(db),
            streams: HashMap::new(),
            next_stream_id: 0,
        }
    }

    /// Reactive database (UI reads + subscribes here).
    pub fn db(&self) -> &ReactiveDatabase {
        &self.db
    }

    pub fn db_mut(&mut self) -> &mut ReactiveDatabase {
        &mut self.db
    }

    pub fn stream_count(&self) -> usize {
        self.streams.len()
    }

    pub fn total_pending(&self) -> usize {
        self.streams.values().map(|s| s.pending_count()).sum()
    }

    pub fn stream_pending_detail(&self) -> Vec<(StreamId, Vec<(u64, usize)>)> {
        self.streams.iter().map(|(id, s)| (*id, s.pending_detail())).collect()
    }

    /// Create a new independent stream.
    pub fn create_stream(&mut self) -> StreamId {
        let id = StreamId(self.next_stream_id);
        self.next_stream_id += 1;
        self.streams.insert(id, Stream::new(id));
        id
    }

    /// Execute a command optimistically on a specific stream.
    /// Fires subscription callbacks for the resulting ZSet.
    pub fn execute(
        &mut self,
        stream_id: StreamId,
        command: C,
    ) -> Result<CommandRequest<C>, SyncClientError> {
        let stream = self.streams.get_mut(&stream_id)
            .ok_or(SyncClientError::UnknownStream(stream_id))?;
        let request = stream.push_command(command, self.db.db_mut_raw())
            .map_err(SyncClientError::CommandError)?;
        self.db.notify(&request.client_zset);
        Ok(request)
    }

    /// Execute a blocking command: creates a temporary stream with a single command.
    pub fn execute_blocking(
        &mut self,
        command: C,
    ) -> Result<(StreamId, CommandRequest<C>), SyncClientError> {
        let stream_id = self.create_stream();
        let request = self.execute(stream_id, command)?;
        Ok((stream_id, request))
    }

    /// Process a server response.
    pub fn receive_response(
        &mut self,
        response: CommandResponse,
    ) -> Result<StreamAction, SyncClientError> {
        let stream_id = response.stream_id;
        let stream = self.streams.get_mut(&stream_id)
            .ok_or(SyncClientError::UnknownStream(stream_id))?;
        let action = stream.receive_response(response);

        match &action {
            StreamAction::AllConfirmed { client_zsets, confirmed_zsets } => {
                // Reconcile in one batched apply: invert each optimistic
                // delta in reverse seq order, then apply each server ZSet
                // in seq order. Concatenated entries flow through one
                // `apply_zset` call so subscribers see a single notify.
                let mut combined = ZSet::new();
                for z in client_zsets.iter().rev() {
                    combined.extend(z.invert());
                }
                for z in confirmed_zsets {
                    combined.extend(z.clone());
                }
                let _ = self.db.apply_zset(&combined);
            }
            StreamAction::Rejected { discarded_zsets, .. } => {
                let mut combined = ZSet::new();
                for z in discarded_zsets.iter().rev() {
                    combined.extend(z.invert());
                }
                let _ = self.db.apply_zset(&combined);
            }
            StreamAction::Idle | StreamAction::WaitingForResponse => {}
        }

        Ok(action)
    }
}
