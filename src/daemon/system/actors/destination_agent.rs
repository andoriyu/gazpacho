use crate::daemon::destination::Destination;
use crate::daemon::ensured::EnsuredDestination;
use crate::daemon::logging::GlobalLogger;
use crate::daemon::system::messages::destination_manager::SaveFromPipe;
use actix::{Actor, Handler, Supervised, SyncContext};
use slog::{debug, o, warn, Logger};
use zstd::Encoder;

pub struct DestinationAgent {
    logger: Logger,
    config: Destination,
}

impl DestinationAgent {
    pub fn new(name: String, config: Destination) -> Self {
        let actor_name = format!("DestinationAgent[{}]", &name);
        let logger = GlobalLogger::get().new(o!("module" => module_path!(), "actor" => actor_name));
        DestinationAgent { logger, config }
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

    fn handle(&mut self, mut msg: SaveFromPipe, _ctx: &mut SyncContext<Self>) -> Self::Result {
        let logger = self
            .logger
            .new(o!("dataset" => msg.dataset.display().to_string(), "snapshot" => msg.snapshot.display().to_string()));
        debug!(logger, "Saving from pipe");
        let mut ensured_dst = EnsuredDestination::ensure(
            &logger,
            &self.config,
            msg.dataset,
            &msg.compression,
            &self.logger,
        )
        .map_err(|e| format!("{}", e))?;
        debug!(logger, "Destination ensured");
        if let Some(ref compression) = msg.compression {
            let mut encoder = Encoder::new(ensured_dst, compression.zstd.level).unwrap();

            if let Err(e) = encoder.multithread(compression.zstd.workers) {
                warn!(logger, "Failed to set zstd multithreading: {}", e);
            }
            let mut encoder = encoder.auto_finish();

            std::io::copy(&mut msg.rx, &mut encoder).unwrap();
        } else {
            std::io::copy(&mut msg.rx, &mut ensured_dst).unwrap();
        }
        debug!(logger, "Closing pipe");
        Ok(())
    }
}
