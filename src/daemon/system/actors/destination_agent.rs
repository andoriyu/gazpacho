use crate::daemon::destination::Destination;
use crate::daemon::ensured::EnsuredDestination;
use crate::daemon::logging::GlobalLogger;
use crate::daemon::system::messages::destination_manager::SaveFromPipe;
use actix::{
    Actor, ActorFuture, Handler, ResponseActFuture, ResponseFuture, Supervised, SyncContext,
    WrapFuture,
};
use slog::{debug, o, warn, Logger};
use std::io::Write;
use zstd::Encoder;

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

impl Handler<SaveFromPipe> for DestinationAgent {
    type Result = Result<(), String>;

    fn handle(&mut self, mut msg: SaveFromPipe, ctx: &mut SyncContext<Self>) -> Self::Result {
        debug!(self.logger, "Starting saving from pipe");
        let mut ensured_dst =
            EnsuredDestination::ensure(&self.config, msg.dataset, &msg.compression);
        if let Some(ref compression) = msg.compression {
            let mut encoder = Encoder::new(ensured_dst, compression.zstd.level).unwrap();

            encoder.multithread(compression.zstd.workers);
            let mut encoder = encoder.auto_finish();

            std::io::copy(&mut msg.rx, &mut encoder).unwrap();
        } else {
            std::io::copy(&mut msg.rx, &mut ensured_dst).unwrap();
        }
        debug!(self.logger, "Closing destination");
        Ok(())
    }
}
