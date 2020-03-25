use slog::Logger;
use crate::daemon::logging::GlobalLogger;
use actix::{Actor, Context, SystemService, Supervised};
use slog::{debug, o, warn, info,error};
use rusqlite::Connection;
use crate::daemon::STARTUP_CONFIGURATION;
use slog_unwraps::ResultExt;
use crate::daemon::system::shutdown;

pub struct TaskRegistry {
    logger: Logger,
    db: Option<Connection>,
}
impl Default for TaskRegistry {
    fn default() -> Self {
        let logger = GlobalLogger::get().new(o!("module" => module_path!(), "actor" => "TaskRegistry"));
        let conf = STARTUP_CONFIGURATION.get().unwrap();
        let db_path = conf.daemon.database.as_path();
        debug!(logger, "Trying to open database at '{}'", db_path.to_string_lossy());
        let mut db = match Connection::open(db_path) {
            Ok(conn) => conn,
            Err(e) => {
                error!(logger, "Failed to open database: {}", e);
                shutdown();
                panic!(e);
            }
        };
        match crate::db::task_registry::runner().run(&mut db) {
            Ok(()) => debug!(logger, "Ran task_registry migrations"),
            Err(e) => {
                error!(logger, "Failed to run task_registry: {}", e);
                shutdown();
                panic!(e);
            }
        };
        TaskRegistry {
            logger,
            db: Some(db),
        }
    }
}

impl Actor for TaskRegistry {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        debug!(self.logger, "Actor started")
    }

    fn stopped(&mut self, ctx: &mut Self::Context) {
        if let Some(db) = self.db.take() {
            match db.close() {
                Ok(()) => {},
                Err((conn, e)) => {
                    error!(self.logger, "Failed to close database: {}", e);
                    self.db = Some(conn);
                }
            };
        }
    }
}

impl SystemService for TaskRegistry {}

impl Supervised for TaskRegistry {
    fn restarting(&mut self, _ctx: &mut Self::Context) {
        warn!(&self.logger, "Actor restarted")
    }
}
