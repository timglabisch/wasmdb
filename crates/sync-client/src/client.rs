use std::collections::HashMap;
use database::Database;
use sync::command::{Command, CommandError};
use sync::protocol::{CommandRequest, CommandResponse, StreamId};
use sync::zset::ZSet;
use crate::stream::{Stream, StreamAction};

pub struct SyncClient<C: Command> {
    /// Confirmed database state (only server-confirmed operations).
    confirmed_db: Database,
    /// Optimistic database state (confirmed + all pending commands).
    /// Rebuilt from confirmed_db on reject.
    optimistic_db: Database,
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
        let optimistic_db = db.clone();
        Self {
            confirmed_db: db,
            optimistic_db,
            streams: HashMap::new(),
            next_stream_id: 0,
        }
    }

    /// The optimistic database — this is what the UI should display.
    pub fn db(&self) -> &Database {
        &self.optimistic_db
    }

    /// Mutable access to the optimistic database (e.g. for queries that need &mut).
    pub fn db_mut(&mut self) -> &mut Database {
        &mut self.optimistic_db
    }

    /// The confirmed database — only contains server-confirmed state.
    pub fn confirmed_db(&self) -> &Database {
        &self.confirmed_db
    }

    /// Mutable access to confirmed DB (for SELECT queries that need &mut).
    pub fn confirmed_db_mut(&mut self) -> &mut Database {
        &mut self.confirmed_db
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
    /// Returns the request to send to the server.
    pub fn execute(
        &mut self,
        stream_id: StreamId,
        command: C,
    ) -> Result<CommandRequest<C>, SyncClientError> {
        let stream = self.streams.get_mut(&stream_id)
            .ok_or(SyncClientError::UnknownStream(stream_id))?;
        let request = stream.push_command(command, &mut self.optimistic_db)
            .map_err(SyncClientError::CommandError)?;
        Ok(request)
    }

    /// Execute a blocking command: creates a temporary stream with a single command.
    /// The caller must send the request, wait for the response, and call receive_response.
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
                // Apply confirmed ZSets to the confirmed database
                for zset in confirmed_zsets {
                    self.apply_zset_to_confirmed(zset);
                }
                // Rebuild optimistic from confirmed + all remaining pending
                self.rebuild_optimistic();
            }
            StreamAction::Rejected { .. } => {
                // Optimistic state is invalid for this stream.
                // Rebuild from confirmed + pending commands of other streams.
                self.rebuild_optimistic();
            }
            StreamAction::Idle | StreamAction::WaitingForResponse => {}
        }

        Ok(action)
    }

    /// Apply a ZSet to the confirmed database.
    fn apply_zset_to_confirmed(&mut self, zset: &ZSet) {
        for entry in &zset.entries {
            if entry.weight > 0 {
                let _ = self.confirmed_db.insert(&entry.table, &entry.row);
            }
            // Deletes would need row lookup by content/PK — simplified for prototype
        }
    }

    /// Rebuild optimistic_db from confirmed_db + re-executing all pending commands.
    fn rebuild_optimistic(&mut self) {
        self.optimistic_db = self.confirmed_db.clone();
        for stream in self.streams.values() {
            for cmd in stream.pending_commands() {
                let _ = cmd.execute(&mut self.optimistic_db);
            }
        }
    }
}
