use crate::daemon::logging::GlobalLogger;
use actix::{Actor, Supervised, SyncContext, SystemService};
use libzetta::zfs::DelegatingZfsEngine;
use slog::{error, o, warn, Logger};

pub struct ZfsManager {
    logger: Logger,
    z: DelegatingZfsEngine,
}

impl Default for ZfsManager {
    fn default() -> Self {
        let logger =
            GlobalLogger::get().new(o!("module" => module_path!(), "actor" => "ZfsManager"));
        let z = match DelegatingZfsEngine::new() {
            Ok(z) => z,
            Err(e) => {
                error!(logger, "Failed to initialize ZFS engine: {}", e);
                panic!("Failed to initialize ZFS engine.")
            }
        };
        ZfsManager { logger, z }
    }
}

impl Actor for ZfsManager {
    type Context = SyncContext<Self>;
}

impl Supervised for ZfsManager {
    fn restarting(&mut self, _ctx: &mut Self::Context) {
        warn!(&self.logger, "Actor restarted")
    }
}
