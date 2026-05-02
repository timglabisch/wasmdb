use database::DbError;
use sql_engine::execute::ExecuteError;
use sql_engine::planner::PlanError;
use sql_parser::parser::ParseError;

#[derive(Debug)]
pub enum SubscribeError {
    Parse(ParseError),
    NotSelect,
    Plan(PlanError),
    Execute(ExecuteError),
    Db(DbError),
}

impl std::fmt::Display for SubscribeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SubscribeError::Parse(e) => write!(f, "parse error: {e}"),
            SubscribeError::NotSelect => write!(f, "subscribe only supports SELECT"),
            SubscribeError::Plan(e) => write!(f, "plan error: {e}"),
            SubscribeError::Execute(e) => write!(f, "execute error: {e}"),
            SubscribeError::Db(e) => write!(f, "db error: {e}"),
        }
    }
}

impl std::error::Error for SubscribeError {}

impl From<DbError> for SubscribeError {
    fn from(e: DbError) -> Self { SubscribeError::Db(e) }
}
impl From<PlanError> for SubscribeError {
    fn from(e: PlanError) -> Self { SubscribeError::Plan(e) }
}
impl From<ExecuteError> for SubscribeError {
    fn from(e: ExecuteError) -> Self { SubscribeError::Execute(e) }
}
impl From<ParseError> for SubscribeError {
    fn from(e: ParseError) -> Self { SubscribeError::Parse(e) }
}
