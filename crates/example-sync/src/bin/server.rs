use std::sync::Arc;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use borsh::BorshDeserialize;
use database::Database;
use sql_engine::schema::{ColumnSchema, DataType, TableSchema};
use sync::command::Command;
use sync::protocol::{CommandRequest, CommandResponse, Verdict};
use sync_server::state::ServerState;
use example_sync_commands::UserCommand;
use tower_http::services::ServeDir;

fn make_db() -> Database {
    let mut db = Database::new();
    db.create_table(TableSchema {
        name: "users".into(),
        columns: vec![
            ColumnSchema { name: "id".into(), data_type: DataType::I64, nullable: false },
            ColumnSchema { name: "name".into(), data_type: DataType::String, nullable: false },
            ColumnSchema { name: "age".into(), data_type: DataType::I64, nullable: false },
        ],
        primary_key: vec![0],
        indexes: vec![],
    }).unwrap();
    db
}

async fn handle_command(
    State(state): State<Arc<ServerState<UserCommand>>>,
    body: Bytes,
) -> impl IntoResponse {
    let request = match CommandRequest::<UserCommand>::try_from_slice(&body) {
        Ok(req) => req,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, e.to_string().into_bytes());
        }
    };

    let mut db = state.db.lock().unwrap();

    let response = match request.command.execute(&mut db) {
        Ok(server_zset) => {
            // Log server-side
            let count = db.execute("SELECT COUNT(users.id) FROM users").unwrap();
            eprintln!("[server] confirmed seq={} | total users: {:?}", request.seq_no.0, count[0]);
            CommandResponse {
                stream_id: request.stream_id,
                seq_no: request.seq_no,
                verdict: Verdict::Confirmed { server_zset },
            }
        }
        Err(e) => {
            eprintln!("[server] rejected seq={}: {}", request.seq_no.0, e);
            CommandResponse {
                stream_id: request.stream_id,
                seq_no: request.seq_no,
                verdict: Verdict::Rejected { reason: e.to_string() },
            }
        }
    };

    let bytes = borsh::to_vec(&response).expect("serialize response");
    (StatusCode::OK, bytes)
}

#[tokio::main]
async fn main() {
    let state = Arc::new(ServerState::<UserCommand>::new(make_db()));

    // Resolve frontend dist dir relative to the crate root
    let static_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("frontend/dist");

    let app = axum::Router::new()
        .route("/command", post(handle_command))
        .nest_service("/", ServeDir::new(&static_dir))
        .with_state(state);

    let addr = "0.0.0.0:3123";
    eprintln!("[server] listening on http://localhost:3123");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
