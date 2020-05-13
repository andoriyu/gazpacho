use crate::daemon::config::Configuration;
use crate::daemon::logging::GlobalLogger;
use crate::daemon::system::messages::maid::Cleanup;
use crate::daemon::CURRENT_CONFIGURATION;
use actix::{Actor, AsyncContext, Context, Handler, SpawnHandle, Supervised, SystemService};
use libzetta::zfs::DelegatingZfsEngine;
use slog::{debug, error, info, o, Logger};

pub struct Maid {
    logger: Logger,
    z: DelegatingZfsEngine,
    tick_handler: Option<SpawnHandle>,
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
            tick_handler: None,
            configuration,
        }
    }
}

impl Actor for Maid {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        debug!(&self.logger, "Actor started");
        if let Some(configuration) = CURRENT_CONFIGURATION.get() {
            if let Some(interval) = configuration.daemon.cleanup_interval {
                debug!(self.logger, "Cleanup interval: {}", interval);
                let duration = interval.to_std().expect("Failed to convert chrono to std");

                let handle = ctx.run_interval(duration, move |this, ctx| {
                    ctx.notify(Cleanup::default());
                });
                self.tick_handler = Some(handle);
            }
        }
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        debug!(&self.logger, "Actor stopped");
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
