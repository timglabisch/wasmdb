use std::collections::HashMap;

use borsh::{BorshSerialize, BorshDeserialize};
use database::{Database, MutResult};
use sql_engine::execute::ParamValue;
use sql_engine::schema::{ColumnSchema, DataType, TableSchema};
use sql_engine::storage::CellValue;
use sync::command::{Command, CommandError};
use sync::protocol::{CommandResponse, Verdict};
use sync::zset::ZSet;
use sync_client::client::SyncClient;
use sync_client::stream::StreamAction;
use sync_server::state::ServerState;

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
enum TestCommand {
    InsertUser { id: i64, name: String, age: i64 },
}

impl Command for TestCommand {
    fn execute_optimistic(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        match self {
            TestCommand::InsertUser { id, name, age } => {
                let mut params: HashMap<String, ParamValue> = HashMap::new();
                params.insert("id".into(), ParamValue::Int(*id));
                params.insert("name".into(), ParamValue::Text(name.clone()));
                params.insert("age".into(), ParamValue::Int(*age));
                match db.execute_mut_with_params(
                    "INSERT INTO users (id, name, age) VALUES (:id, :name, :age)",
                    params,
                ) {
                    Ok(MutResult::Mutation(z)) => Ok(z),
                    Ok(_) => Ok(ZSet::new()),
                    Err(e) => Err(CommandError::ExecutionFailed(e.to_string())),
                }
            }
        }
    }
}

fn make_schema() -> TableSchema {
    TableSchema {
        name: "users".into(),
        columns: vec![
            ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
            ColumnSchema { name: "name".into(), data_type: DataType::String, nullable: false },
            ColumnSchema { name: "age".into(), data_type: DataType::I64, nullable: false },
        ],
        primary_key: vec![0],
        indexes: vec![],
    }
}

fn make_db() -> Database {
    let mut db = Database::new();
    db.create_table(make_schema()).unwrap();
    db
}

/// Simulate the server handler without HTTP: apply the client's optimistic
/// zset against the server-side `Database` and echo it back on success.
async fn server_roundtrip(
    state: &ServerState<TestCommand>,
    request_bytes: &[u8],
) -> CommandResponse {
    use sync::protocol::CommandRequest;

    let request: CommandRequest<TestCommand> = borsh::from_slice(request_bytes).unwrap();
    let mut db = state.db.lock().await;

    match db.apply_zset(&request.client_zset) {
        Ok(()) => CommandResponse {
            stream_id: request.stream_id,
            seq_no: request.seq_no,
            verdict: Verdict::Confirmed { server_zset: request.client_zset },
        },
        Err(e) => CommandResponse {
            stream_id: request.stream_id,
            seq_no: request.seq_no,
            verdict: Verdict::Rejected { reason: e.to_string() },
        },
    }
}

#[tokio::test]
async fn e2e_single_command() {
    let mut client = SyncClient::<TestCommand>::new(make_db());
    let server = ServerState::<TestCommand>::new(make_db());
    let stream = client.create_stream();

    // Client executes command
    let cmd = TestCommand::InsertUser { id: 1, name: "Alice".into(), age: 30 };
    let request = client.execute(stream, cmd).unwrap();

    // Serialize, send to server, get response
    let request_bytes = borsh::to_vec(&request).unwrap();
    let response = server_roundtrip(&server, &request_bytes).await;

    // Client processes response
    let action = client.receive_response(response).unwrap();
    assert!(matches!(action, StreamAction::AllConfirmed { .. }));

    // Both databases should have the same data
    let client_result = client.db_mut().execute("SELECT users.name FROM users").unwrap();
    let server_result = server.db.lock().await.execute("SELECT users.name FROM users").unwrap();
    assert_eq!(client_result, server_result);
    assert_eq!(client_result[0], vec![CellValue::Str("Alice".into())]);
}

#[tokio::test]
async fn e2e_multiple_commands_same_stream() {
    let mut client = SyncClient::<TestCommand>::new(make_db());
    let server = ServerState::<TestCommand>::new(make_db());
    let stream = client.create_stream();

    let cmd1 = TestCommand::InsertUser { id: 1, name: "Alice".into(), age: 30 };
    let cmd2 = TestCommand::InsertUser { id: 2, name: "Bob".into(), age: 25 };
    let cmd3 = TestCommand::InsertUser { id: 3, name: "Carol".into(), age: 35 };

    let req1 = client.execute(stream, cmd1).unwrap();
    let req2 = client.execute(stream, cmd2).unwrap();
    let req3 = client.execute(stream, cmd3).unwrap();

    // Server processes all
    let resp1 = server_roundtrip(&server, &borsh::to_vec(&req1).unwrap()).await;
    let resp2 = server_roundtrip(&server, &borsh::to_vec(&req2).unwrap()).await;
    let resp3 = server_roundtrip(&server, &borsh::to_vec(&req3).unwrap()).await;

    // Client receives out of order
    let action = client.receive_response(resp3).unwrap();
    assert!(matches!(action, StreamAction::WaitingForResponse));

    let action = client.receive_response(resp1).unwrap();
    assert!(matches!(action, StreamAction::WaitingForResponse));

    let action = client.receive_response(resp2).unwrap();
    assert!(matches!(action, StreamAction::AllConfirmed { .. }));

    // Verify identical state
    let client_result = client.db_mut().execute("SELECT users.name FROM users").unwrap();
    let server_result = server.db.lock().await.execute("SELECT users.name FROM users").unwrap();
    assert_eq!(client_result, server_result);
    assert_eq!(client_result[0].len(), 3);
}

#[tokio::test]
async fn e2e_two_streams_one_rejected() {
    let mut client = SyncClient::<TestCommand>::new(make_db());
    let server = ServerState::<TestCommand>::new(make_db());
    let stream_a = client.create_stream();
    let stream_b = client.create_stream();

    let cmd_a = TestCommand::InsertUser { id: 1, name: "Alice".into(), age: 30 };
    let cmd_b = TestCommand::InsertUser { id: 2, name: "Bob".into(), age: 25 };

    let req_a = client.execute(stream_a, cmd_a).unwrap();
    let req_b = client.execute(stream_b, cmd_b).unwrap();

    // Server confirms B but rejects A
    let resp_b = server_roundtrip(&server, &borsh::to_vec(&req_b).unwrap()).await;
    let resp_a = CommandResponse {
        stream_id: req_a.stream_id,
        seq_no: req_a.seq_no,
        verdict: Verdict::Rejected { reason: "server conflict".into() },
    };

    let action = client.receive_response(resp_b).unwrap();
    assert!(matches!(action, StreamAction::AllConfirmed { .. }));

    let action = client.receive_response(resp_a).unwrap();
    assert!(matches!(action, StreamAction::Rejected { .. }));

    // Client should only have Bob (from confirmed stream B)
    let result = client.db_mut().execute("SELECT users.name FROM users").unwrap();
    assert_eq!(result[0], vec![CellValue::Str("Bob".into())]);
}

#[test]
fn e2e_borsh_wire_format() {
    // Verify that the full borsh roundtrip works for protocol types
    use sync::protocol::{CommandRequest, StreamId, SeqNo};

    let mut zset = ZSet::new();
    zset.insert("users".into(), vec![CellValue::I64(1), CellValue::Str("Test".into()), CellValue::I64(42)]);

    let request = CommandRequest {
        stream_id: StreamId(0),
        seq_no: SeqNo(0),
        command: TestCommand::InsertUser { id: 1, name: "Test".into(), age: 42 },
        client_zset: zset.clone(),
    };

    let bytes = borsh::to_vec(&request).unwrap();
    let decoded: CommandRequest<TestCommand> = borsh::from_slice(&bytes).unwrap();

    assert_eq!(decoded.stream_id, request.stream_id);
    assert_eq!(decoded.seq_no, request.seq_no);
    assert_eq!(decoded.client_zset, request.client_zset);

    let response = CommandResponse {
        stream_id: StreamId(0),
        seq_no: SeqNo(0),
        verdict: Verdict::Confirmed { server_zset: zset },
    };

    let bytes = borsh::to_vec(&response).unwrap();
    let decoded: CommandResponse = borsh::from_slice(&bytes).unwrap();
    assert!(matches!(decoded.verdict, Verdict::Confirmed { .. }));
}
