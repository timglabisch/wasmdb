use sql_engine::execute::ExecuteError;
use sql_engine::planner::PlanError;

#[derive(Debug)]
pub enum DbError {
    Parse(String),
    Plan(PlanError),
    Execute(ExecuteError),
    TableAlreadyExists(String),
    TableNotFound(String),
    /// The SELECT references a `schema.fn(args)` source that needs Phase 0
    /// fetcher resolution; call one of the `*_async` methods instead.
    RequiresAsync,
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::Parse(msg) => write!(f, "parse error: {msg}"),
            DbError::Plan(e) => write!(f, "plan error: {e}"),
            DbError::Execute(e) => write!(f, "execute error: {e}"),
            DbError::TableAlreadyExists(t) => write!(f, "table already exists: {t}"),
            DbError::TableNotFound(t) => write!(f, "table not found: {t}"),
            DbError::RequiresAsync => write!(
                f,
                "query references a fetcher source; use an async execute method",
            ),
        }
    }
}

impl std::error::Error for DbError {}

impl From<PlanError> for DbError {
    fn from(e: PlanError) -> Self { DbError::Plan(e) }
}

impl From<ExecuteError> for DbError {
    fn from(e: ExecuteError) -> Self { DbError::Execute(e) }
}
