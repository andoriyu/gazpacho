use crate::daemon::config::Configuration;
use crate::daemon::logging::GlobalLogger;
use crate::daemon::system::messages::maid::Cleanup;
use crate::daemon::CURRENT_CONFIGURATION;
use actix::{Actor, Context, Handler, Supervised, SystemService};
use libzetta::zfs::DelegatingZfsEngine;
use slog::{error, info, o, trace, Logger};

pub struct Maid {
    logger: Logger,
    z: DelegatingZfsEngine,
    configuration: Configuration,
}

impl Default for Maid {
    fn default() -> Self {
        let logger = GlobalLogger::get().new(o!("module" => module_path!(), "actor" => "Maid"));
        let z = match DelegatingZfsEngine::new() {
            Ok(z) => z,
            Err(e) => {
                error!(logger, "Failed to initialize ZFS engine: {}", e);
                panic!("Failed to initialize ZFS engine.")
            }
        };
        let configuration = CURRENT_CONFIGURATION.get().cloned().unwrap();
        Self {
            logger,
            z,
            configuration,
        }
    }
}

impl Actor for Maid {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        trace!(&self.logger, "Actor started");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        trace!(&self.logger, "Actor stopped");
    }
}

impl SystemService for Maid {}

impl Supervised for Maid {}

impl Handler<Cleanup> for Maid {
    type Result = ();

    fn handle(&mut self, msg: Cleanup, _ctx: &mut Context<Self>) -> Self::Result {
        info!(self.logger, "Performing cleanup");
    }
}
