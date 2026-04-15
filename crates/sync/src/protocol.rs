use borsh::{BorshSerialize, BorshDeserialize};
use crate::zset::ZSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, BorshSerialize, BorshDeserialize)]
pub struct StreamId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, BorshSerialize, BorshDeserialize)]
pub struct SeqNo(pub u64);

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct CommandRequest<C> {
    pub stream_id: StreamId,
    pub seq_no: SeqNo,
    pub command: C,
    pub client_zset: ZSet,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct CommandResponse {
    pub stream_id: StreamId,
    pub seq_no: SeqNo,
    pub verdict: Verdict,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub enum Verdict {
    Confirmed { server_zset: ZSet },
    Rejected { reason: String },
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct BatchCommandRequest<C> {
    pub requests: Vec<CommandRequest<C>>,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct BatchCommandResponse {
    pub responses: Vec<CommandResponse>,
}
