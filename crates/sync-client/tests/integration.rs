use borsh::{BorshSerialize, BorshDeserialize};
use database::Database;
use sql_engine::schema::{ColumnSchema, DataType, TableSchema};
use sql_engine::storage::CellValue;
use sync::command::{Command, CommandError};
use sync::protocol::{CommandResponse, Verdict};
use sync::zset::ZSet;
use sync_client::client::SyncClient;
use sync_client::stream::StreamAction;

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
enum TestCommand {
    InsertUser { id: i64, name: String, age: i64 },
}

impl Command for TestCommand {
    fn execute(&self, db: &mut Database) -> Result<ZSet, CommandError> {
        let mut zset = ZSet::new();
        match self {
            TestCommand::InsertUser { id, name, age } => {
                let row = vec![
                    CellValue::I64(*id),
                    CellValue::Str(name.clone()),
                    CellValue::I64(*age),
                ];
                db.insert("users", &row)
                    .map_err(|e| CommandError::ExecutionFailed(e.to_string()))?;
                zset.insert("users".into(), row);
            }
        }
        Ok(zset)
    }
}

fn make_db() -> Database {
    let mut db = Database::new();
    let schema = TableSchema {
        name: "users".into(),
        columns: vec![
            ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
            ColumnSchema { name: "name".into(), data_type: DataType::String, nullable: false },
            ColumnSchema { name: "age".into(), data_type: DataType::I64, nullable: false },
        ],
        primary_key: vec![0],
        indexes: vec![],
    };
    db.create_table(schema).unwrap();
    db
}

/// Simulate server execution: run the command on a separate DB, return the response.
fn simulate_server(
    server_db: &mut Database,
    request: &sync::protocol::CommandRequest<TestCommand>,
) -> CommandResponse {
    match request.command.execute(server_db) {
        Ok(server_zset) => CommandResponse {
            stream_id: request.stream_id,
            seq_no: request.seq_no,
            verdict: Verdict::Confirmed { server_zset },
        },
        Err(e) => CommandResponse {
            stream_id: request.stream_id,
            seq_no: request.seq_no,
            verdict: Verdict::Rejected { reason: e.to_string() },
        },
    }
}

#[test]
fn test_single_command_confirmed() {
    let mut client = SyncClient::<TestCommand>::new(make_db());
    let mut server_db = make_db();
    let stream = client.create_stream();

    let cmd = TestCommand::InsertUser { id: 1, name: "Alice".into(), age: 30 };
    let request = client.execute(stream, cmd).unwrap();

    // Optimistic DB should already have the row
    let result = client.db_mut().execute("SELECT users.name FROM users").unwrap();
    assert_eq!(result[0], vec![CellValue::Str("Alice".into())]);

    // Server confirms
    let response = simulate_server(&mut server_db, &request);
    let action = client.receive_response(response).unwrap();

    assert!(matches!(action, StreamAction::AllConfirmed { .. }));
}

#[test]
fn test_multiple_commands_buffered() {
    let mut client = SyncClient::<TestCommand>::new(make_db());
    let mut server_db = make_db();
    let stream = client.create_stream();

    let cmd1 = TestCommand::InsertUser { id: 1, name: "Alice".into(), age: 30 };
    let cmd2 = TestCommand::InsertUser { id: 2, name: "Bob".into(), age: 25 };

    let req1 = client.execute(stream, cmd1).unwrap();
    let req2 = client.execute(stream, cmd2).unwrap();

    // Confirm cmd1 — should still wait for cmd2
    let resp1 = simulate_server(&mut server_db, &req1);
    let action = client.receive_response(resp1).unwrap();
    assert!(matches!(action, StreamAction::WaitingForResponse));

    // Confirm cmd2 — now all confirmed
    let resp2 = simulate_server(&mut server_db, &req2);
    let action = client.receive_response(resp2).unwrap();
    assert!(matches!(action, StreamAction::AllConfirmed { .. }));
}

#[test]
fn test_out_of_order_confirmation() {
    let mut client = SyncClient::<TestCommand>::new(make_db());
    let mut server_db = make_db();
    let stream = client.create_stream();

    let cmd1 = TestCommand::InsertUser { id: 1, name: "Alice".into(), age: 30 };
    let cmd2 = TestCommand::InsertUser { id: 2, name: "Bob".into(), age: 25 };

    let req1 = client.execute(stream, cmd1).unwrap();
    let req2 = client.execute(stream, cmd2).unwrap();

    // Confirm cmd2 first — should wait
    let resp2 = simulate_server(&mut server_db, &req2);
    let action = client.receive_response(resp2).unwrap();
    assert!(matches!(action, StreamAction::WaitingForResponse));

    // Now confirm cmd1 — both should flush
    let resp1 = simulate_server(&mut server_db, &req1);
    let action = client.receive_response(resp1).unwrap();
    assert!(matches!(action, StreamAction::AllConfirmed { .. }));
}

#[test]
fn test_reject_discards_stream() {
    let mut client = SyncClient::<TestCommand>::new(make_db());
    let stream = client.create_stream();

    let cmd1 = TestCommand::InsertUser { id: 1, name: "Alice".into(), age: 30 };
    let cmd2 = TestCommand::InsertUser { id: 2, name: "Bob".into(), age: 25 };

    let req1 = client.execute(stream, cmd1).unwrap();
    let _req2 = client.execute(stream, cmd2).unwrap();

    // Server rejects cmd1
    let response = CommandResponse {
        stream_id: req1.stream_id,
        seq_no: req1.seq_no,
        verdict: Verdict::Rejected { reason: "conflict".into() },
    };
    let action = client.receive_response(response).unwrap();
    assert!(matches!(action, StreamAction::Rejected { .. }));

    // After reject, optimistic DB should be clean (no pending rows)
    let result = client.db_mut().execute("SELECT users.name FROM users").unwrap();
    assert_eq!(result[0].len(), 0);
}

#[test]
fn test_multi_stream_independence() {
    let mut client = SyncClient::<TestCommand>::new(make_db());
    let mut server_db = make_db();
    let stream_a = client.create_stream();
    let stream_b = client.create_stream();

    let cmd_a = TestCommand::InsertUser { id: 1, name: "Alice".into(), age: 30 };
    let cmd_b = TestCommand::InsertUser { id: 2, name: "Bob".into(), age: 25 };

    let req_a = client.execute(stream_a, cmd_a).unwrap();
    let req_b = client.execute(stream_b, cmd_b).unwrap();

    // Both should be in optimistic DB
    let result = client.db_mut().execute("SELECT users.name FROM users").unwrap();
    assert_eq!(result[0].len(), 2);

    // Reject stream A
    let response_a = CommandResponse {
        stream_id: req_a.stream_id,
        seq_no: req_a.seq_no,
        verdict: Verdict::Rejected { reason: "conflict".into() },
    };
    let action = client.receive_response(response_a).unwrap();
    assert!(matches!(action, StreamAction::Rejected { .. }));

    // Bob (stream B) should still be in optimistic DB
    let result = client.db_mut().execute("SELECT users.name FROM users").unwrap();
    assert_eq!(result[0], vec![CellValue::Str("Bob".into())]);

    // Confirm stream B
    let response_b = simulate_server(&mut server_db, &req_b);
    let action = client.receive_response(response_b).unwrap();
    assert!(matches!(action, StreamAction::AllConfirmed { .. }));
}

#[test]
fn test_blocking_command() {
    let mut client = SyncClient::<TestCommand>::new(make_db());
    let mut server_db = make_db();

    let cmd = TestCommand::InsertUser { id: 1, name: "Alice".into(), age: 30 };
    let (_stream_id, request) = client.execute_blocking(cmd).unwrap();

    let response = simulate_server(&mut server_db, &request);
    let action = client.receive_response(response).unwrap();
    assert!(matches!(action, StreamAction::AllConfirmed { .. }));
}

#[test]
fn test_borsh_roundtrip() {
    let cmd = TestCommand::InsertUser { id: 42, name: "Test".into(), age: 99 };
    let bytes = borsh::to_vec(&cmd).unwrap();
    let decoded: TestCommand = borsh::from_slice(&bytes).unwrap();
    assert!(matches!(decoded, TestCommand::InsertUser { id: 42, .. }));
}

#[test]
fn test_zset_borsh_roundtrip() {
    let mut zset = ZSet::new();
    zset.insert("users".into(), vec![CellValue::I64(1), CellValue::Str("Alice".into())]);
    zset.delete("users".into(), vec![CellValue::I64(2), CellValue::Str("Bob".into())]);

    let bytes = borsh::to_vec(&zset).unwrap();
    let decoded: ZSet = borsh::from_slice(&bytes).unwrap();
    assert_eq!(zset, decoded);
}
