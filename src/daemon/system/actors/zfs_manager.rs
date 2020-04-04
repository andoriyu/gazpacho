use crate::daemon::logging::GlobalLogger;
use crate::daemon::system::messages::zfs_manager::{
    GetDatasetsForTask, MakeSnapshots, SendSnapshotToPipe,
};
use actix::{Actor, Handler, MessageResult, Supervised, SyncContext};
use libzetta::zfs::{DelegatingZfsEngine, SendFlags, ZfsEngine};
use regex::Regex;
use slog::{debug, error, o, warn, Logger};
use std::path::PathBuf;

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

impl Handler<GetDatasetsForTask> for ZfsManager {
    type Result = MessageResult<GetDatasetsForTask>;

    fn handle(&mut self, msg: GetDatasetsForTask, _ctx: &mut SyncContext<Self>) -> Self::Result {
        let filter = {
            if let Ok(re) = Regex::new(&msg.filter) {
                re
            } else {
                error!(self.logger, "Failed to compile regex: {}", &msg.filter);
                return MessageResult(Vec::new());
            }
        };

        let volumes = self.z.list_volumes(&msg.zpool).unwrap_or_default();
        let filesystems = self.z.list_filesystems(&msg.zpool).unwrap_or_default();
        MessageResult(
            volumes
                .into_iter()
                .chain(filesystems)
                .filter(|dataset| filter.is_match(dataset.to_string_lossy().as_ref()))
                .collect(),
        )
    }
}

impl Handler<MakeSnapshots> for ZfsManager {
    type Result = Result<(), String>;

    fn handle(&mut self, msg: MakeSnapshots, _ctx: &mut SyncContext<Self>) -> Self::Result {
        let snapshots: Vec<(PathBuf, PathBuf)> = msg
            .datasets
            .iter()
            .map(|dataset| {
                let name = format!("{}@{}", dataset.to_str().unwrap(), &msg.snapshot);
                (dataset.clone(), PathBuf::from(name))
            })
            .collect();
        let snapshots_to_create: Vec<PathBuf> = snapshots
            .iter()
            .map(|(_, s)| s)
            .cloned()
            .filter(|s| self.z.exists(s).unwrap() == false)
            .collect();

        match self.z.snapshot(&snapshots_to_create, None) {
            Ok(()) => Ok(()),
            Err(e) => {
                error!(self.logger, "Failed to create snapshots: {}", e);
                Err(e.to_string())
            }
        }
    }
}

impl Handler<SendSnapshotToPipe> for ZfsManager {
    type Result = Result<(), String>;

    fn handle(&mut self, msg: SendSnapshotToPipe, _ctx: &mut SyncContext<Self>) -> Self::Result {
        let result = if let Some(source) = msg.1 {
            debug!(
                self.logger,
                "Sending incremental snapshot from {} to {} to pipe",
                &source.display(),
                msg.0.display()
            );
            self.z
                .send_incremental(&msg.0, &source, msg.2.write, SendFlags::default())
        } else {
            debug!(
                self.logger,
                "Sending full snapshot for {} to pipe",
                msg.0.display()
            );
            self.z.send_full(&msg.0, msg.2.write, SendFlags::default())
        };
        match result {
            Ok(()) => {
                debug!(self.logger, "Sent {}", msg.0.display());
                Ok(())
            }
            Err(e) => {
                error!(
                    self.logger,
                    "Error sending snapshopt \"{}\": {}",
                    &msg.0.display(),
                    &e
                );
                Err(format!("{}", e))
            }
        }
    }
}
