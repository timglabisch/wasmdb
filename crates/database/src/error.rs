use sql_engine::execute::ExecuteError;
use sql_engine::planner::PlanError;

#[derive(Debug)]
pub enum DbError {
    Parse(String),
    Plan(PlanError),
    Execute(ExecuteError),
    TableAlreadyExists(String),
    TableNotFound(String),
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::Parse(msg) => write!(f, "parse error: {msg}"),
            DbError::Plan(e) => write!(f, "plan error: {e}"),
            DbError::Execute(e) => write!(f, "execute error: {e}"),
            DbError::TableAlreadyExists(t) => write!(f, "table already exists: {t}"),
            DbError::TableNotFound(t) => write!(f, "table not found: {t}"),
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
