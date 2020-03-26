use crate::daemon::destination::Destination;
use crate::daemon::logging::GlobalLogger;
use actix::{Actor, Supervised, SyncContext};
use slog::{debug, o, warn, Logger};

pub struct DestinationAgent {
    name: String,
    logger: Logger,
    config: Destination,
}

impl DestinationAgent {
    pub fn new(name: String, config: Destination) -> Self {
        let actor_name = format!("DestionationAgent[{}]", &name);
        let logger = GlobalLogger::get().new(o!("module" => module_path!(), "actor" => actor_name));
        DestinationAgent {
            name,
            logger,
            config,
        }
    }
}
impl Actor for DestinationAgent {
    type Context = SyncContext<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        debug!(self.logger, "Actor started");
    }
}

impl Supervised for DestinationAgent {
    fn restarting(&mut self, _ctx: &mut Self::Context) {
        warn!(&self.logger, "Actor restarted");
    }
}
