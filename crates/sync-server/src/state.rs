use database::Database;
use sync::command::Command;
use tokio::sync::Mutex;

pub struct ServerState<C: Command> {
    pub db: Mutex<Database>,
    _phantom: std::marker::PhantomData<C>,
}

impl<C: Command> ServerState<C> {
    pub fn new(db: Database) -> Self {
        Self {
            db: Mutex::new(db),
            _phantom: std::marker::PhantomData,
        }
    }
}
