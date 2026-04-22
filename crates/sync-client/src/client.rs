use std::collections::HashMap;
use database::Database;
use database_reactive::ReactiveDatabase;
use sync::command::{Command, CommandError};
use sync::protocol::{CommandRequest, CommandResponse, StreamId};
use sync::zset::ZSet;
use crate::stream::{Stream, StreamAction};

pub struct SyncClient<C: Command> {
    /// Confirmed database state (only server-confirmed operations). Passive —
    /// no subscriptions fire from mutations applied here.
    confirmed_db: Database,
    /// Optimistic database state (confirmed + all pending commands).
    /// Owns the subscription registry + callbacks. Rebuilt from confirmed_db
    /// on reject.
    optimistic_db: ReactiveDatabase,
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
        let optimistic_db = ReactiveDatabase::from_database(db.clone());
        Self {
            confirmed_db: db,
            optimistic_db,
            streams: HashMap::new(),
            next_stream_id: 0,
        }
    }

    /// Optimistic database (what the UI should display) — full reactive API.
    pub fn db(&self) -> &ReactiveDatabase {
        &self.optimistic_db
    }

    pub fn db_mut(&mut self) -> &mut ReactiveDatabase {
        &mut self.optimistic_db
    }

    /// Confirmed database (server-acknowledged state only).
    pub fn confirmed_db(&self) -> &Database {
        &self.confirmed_db
    }

    pub fn confirmed_db_mut(&mut self) -> &mut Database {
        &mut self.confirmed_db
    }

    // Subscribe/unsubscribe/on_dirty/next_dirty live on the inner
    // ReactiveDatabase — consumers reach them via `db()` / `db_mut()`.
    // SyncClient no longer duplicates the reactivity API surface.

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
        let request = stream.push_command(command, self.optimistic_db.db_mut_raw())
            .map_err(SyncClientError::CommandError)?;
        self.optimistic_db.notify(&request.client_zset);
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
            StreamAction::AllConfirmed { confirmed_zsets } => {
                for zset in confirmed_zsets {
                    self.apply_zset_to_confirmed(zset);
                }
                self.rebuild_optimistic();
            }
            StreamAction::Rejected { .. } => {
                self.rebuild_optimistic();
            }
            StreamAction::Idle | StreamAction::WaitingForResponse => {}
        }

        Ok(action)
    }

    fn apply_zset_to_confirmed(&mut self, zset: &ZSet) {
        let _ = self.confirmed_db.apply_zset(zset);
    }

    /// Rebuild optimistic_db from confirmed_db + re-executing all pending commands.
    /// Subscriptions are preserved (only the inner table data is replaced).
    /// Fires a brute-force `notify_all()` afterwards.
    fn rebuild_optimistic(&mut self) {
        self.optimistic_db.replace_data(&self.confirmed_db);
        for stream in self.streams.values() {
            for cmd in stream.pending_commands() {
                let _ = cmd.execute_optimistic(self.optimistic_db.db_mut_raw());
            }
        }
        self.optimistic_db.notify_all();
    }
}
