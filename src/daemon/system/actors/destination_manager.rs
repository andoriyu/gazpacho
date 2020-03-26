use crate::daemon::logging::GlobalLogger;
use crate::daemon::system::actors::destination_agent::DestinationAgent;
use crate::daemon::system::messages::destination_manager::NewDestinations;
use actix::{Actor, Addr, Context, Handler, Supervised, SyncArbiter, SystemService};
use slog::{debug, o, warn, Logger};
use std::collections::HashMap;

pub struct DestinationManager {
    logger: Logger,
    destinations: HashMap<String, Addr<DestinationAgent>>,
}
impl Default for DestinationManager {
    fn default() -> Self {
        let logger = GlobalLogger::get()
            .new(o!("module" => module_path!(), "actor" => "DestinationManager"));
        DestinationManager {
            logger,
            destinations: HashMap::new(),
        }
    }
}

impl Actor for DestinationManager {
    type Context = Context<Self>;
}
impl Supervised for DestinationManager {
    fn restarting(&mut self, _ctx: &mut Self::Context) {
        warn!(&self.logger, "Actor restarted")
    }
}

impl SystemService for DestinationManager {}

impl Handler<NewDestinations> for DestinationManager {
    type Result = ();

    fn handle(&mut self, msg: NewDestinations, _ctx: &mut Context<Self>) -> Self::Result {
        debug!(self.logger, "Updating destination list");
        let destinations = msg
            .0
            .into_iter()
            .map(|(name, conf)| {
                let n = name.clone();
                let addr = SyncArbiter::start(conf.parallelism as usize, move || {
                    DestinationAgent::new(name.clone(), conf.clone())
                });
                (n, addr)
            })
            .collect();
        self.destinations = destinations;
    }
}
