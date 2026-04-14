use std::sync::Mutex;
use database::Database;
use sync::command::Command;

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
