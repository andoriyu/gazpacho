use crate::daemon::config::Task;
use crate::daemon::logging::GlobalLogger;
use crate::daemon::system::actors::destination_manager::DestinationManager;
use crate::daemon::system::actors::zfs_manager::ZfsManager;
use crate::daemon::system::messages::destination_manager::{NewDestinations, SaveFromPipe};
use crate::daemon::system::messages::task_manager::{ExecuteTask, NewConfiguration};
use crate::daemon::system::messages::zfs_manager::{
    GetDatasetsForTask, MakeSnapshots, SendSnapshotToPipe,
};
use crate::daemon::system::shutdown;
use crate::daemon::STARTUP_CONFIGURATION;
use actix::{Actor, Addr, AsyncContext, Context, Handler,  Supervised, SyncArbiter, SystemService, ResponseFuture};
use chrono::Utc;
use filedescriptor::Pipe;
use rusqlite::Connection;
use slog::Logger;
use slog::{debug, error, info, o, warn};
use std::collections::HashMap;
use std::path::PathBuf;
use futures::stream::FuturesUnordered;
use futures::{StreamExt};
use tokio::sync::Semaphore;


pub struct TaskManager {
    logger: Logger,
    db: Option<Connection>,
    tasks: HashMap<String, Task>,
    zfs_manager: Addr<ZfsManager>,
}
impl Default for TaskManager {
    fn default() -> Self {
        let logger =
            GlobalLogger::get().new(o!("module" => module_path!(), "actor" => "TaskRegistry"));
        let conf = STARTUP_CONFIGURATION.get().unwrap();
        let db_path = conf.daemon.database.as_path();
        debug!(
            logger,
            "Trying to open database at '{}'",
            db_path.to_string_lossy()
        );
        let mut db = match Connection::open(db_path) {
            Ok(conn) => conn,
            Err(e) => {
                error!(logger, "Failed to open database: {}", e);
                shutdown();
                Connection::open_in_memory().unwrap()
            }
        };
        match crate::db::task_manager::runner().run(&mut db) {
            Ok(()) => debug!(logger, "Ran task_manager migrations"),
            Err(e) => {
                error!(logger, "Failed to run task_manager: {}", e);
                shutdown();
            }
        };

        let zfs_manager = SyncArbiter::start(conf.parallelism as usize, ZfsManager::default);

        TaskManager {
            logger,
            db: Some(db),
            tasks: HashMap::new(),
            zfs_manager,
        }
    }
}

impl Actor for TaskManager {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        debug!(self.logger, "Actor started");
        ctx.address().do_send(NewConfiguration(
            STARTUP_CONFIGURATION.get().cloned().unwrap(),
        ))
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        if let Some(db) = self.db.take() {
            match db.close() {
                Ok(()) => {}
                Err((conn, e)) => {
                    error!(self.logger, "Failed to close database: {}", e);
                    self.db = Some(conn);
                }
            };
        }
    }
}

impl SystemService for TaskManager {}

impl Supervised for TaskManager {
    fn restarting(&mut self, _ctx: &mut Self::Context) {
        warn!(&self.logger, "Actor restarted")
    }
}

impl Handler<NewConfiguration> for TaskManager {
    type Result = ();

    fn handle(&mut self, msg: NewConfiguration, _ctx: &mut Context<Self>) -> Self::Result {
        debug!(self.logger, "Loading new configuration");
        let tasks: HashMap<String, Task> = msg
            .0
            .tasks
            .clone()
            .into_iter()
            .filter(|(name, task)| {
                let dst = task.destination.as_str();
                if msg.0.destinations.contains_key(dst) {
                    true
                } else {
                    error!(
                        self.logger,
                        "Task '{}' specified a non-existent destination '{}' and will be skipped.",
                        name,
                        dst
                    );
                    false
                }
            })
            .collect();
        debug!(
            self.logger,
            "Loaded tasks: {:?}",
            tasks.keys().collect::<Vec<&String>>()
        );
        let used_destinations: Vec<String> = tasks
            .iter()
            .map(|(_, task)| task.destination.clone())
            .collect();
        self.tasks = tasks;

        let dsts = msg
            .0
            .destinations
            .clone()
            .into_iter()
            .filter(|(name, _)| used_destinations.contains(name))
            .collect();
        let new_destinations = NewDestinations(dsts);
        let dst_manager = DestinationManager::from_registry();
        dst_manager.do_send(new_destinations);
    }
}
async fn process_task(task_name: String, maybe_task: Option<Task>, logger: Logger, zfs_addr: Addr<ZfsManager>) -> Result<(), String> {
    if let Some(task) = maybe_task {
        let context = task.full_replication.as_ref().unwrap();
        let req =
            GetDatasetsForTask::new(context.zpool.clone(), context.filter.clone());
        let res = zfs_addr.send(req).await.unwrap();
        if res.is_empty() {
            warn!(logger, "Got no datasets to work with")
        } else {
            debug!(logger, "Got {} datasets to work with", res.len());
        }
        let mut has_errors = false;

        let snapshot_name = get_snapshot_name();
        let req = MakeSnapshots::new(res.clone(), snapshot_name.clone());
        let snap_result = zfs_addr.send(req).await.unwrap();
        if let Err(e) = snap_result {
            return Err(e);
        }

        let dst_manager = DestinationManager::from_registry();
        let semaphore = Semaphore::new(task.parallelism as usize);
        let mut steps = FuturesUnordered::new();
        for dataset in res {
            steps.push(process_dataset(&logger, &zfs_addr, &task, &snapshot_name, &dst_manager, &semaphore, dataset));
        }

        while let Some(result) = steps.next().await {
            if !has_errors {
                continue;
            }
            if let Err(_) = result {
                has_errors = true;
            }
        }
        if has_errors {
            Err("Completed with errors".to_string())
        } else {
            info!(logger, "Done");
            Ok(())
        }
    } else {
        Err(format!("Task {} not found", task_name.as_str()))
    }
}

async fn process_dataset(logger: &Logger, zfs_addr: &Addr<ZfsManager>, task: &Task, snapshot_name: &String, dst_manager: &Addr<DestinationManager>, semaphore: &Semaphore, dataset: PathBuf) -> Result<(), ()> {
    let logger = logger.new(o!("dataset" => dataset.to_string_lossy().to_string()));
    debug!(logger, "Waiting for a permit to work on {}", dataset.to_string_lossy());
    let _permit = semaphore.acquire().await;
    debug!(logger, "Got the permit the work on {}", dataset.to_string_lossy());
    let snapshot = PathBuf::from(format!(
        "{}@{}",
        dataset.to_string_lossy(),
        &snapshot_name
    ));
    let pipe = Pipe::new().unwrap();
    let dst_req = SaveFromPipe::new(
        task.destination.clone(),
        dataset.clone(),
        snapshot.clone(),
        task.compression.clone(),
        pipe.read.try_clone().unwrap(),
    );
    let dst_res = dst_manager.send(dst_req);
    let zfs_req = SendSnapshotToPipe(snapshot.clone(), pipe);
    let zfs_res = zfs_addr.send(zfs_req);
    let result_both = futures::join!(dst_res, zfs_res);
    let mut ret = Ok(());
    match result_both.0 {
        Ok(result) => {
            match result {
                Ok(()) => {},
                Err(e) => {
                    ret = Err(());
                    error!(logger, "{}", e);
                }
            }
        },
        Err(e) => {
            ret = Err(());
            error!(logger, "Failed to send a message to destination manager: {}", e);
        }
    };
    match result_both.1 {
        Ok(result) => {
            match result {
                Ok(()) => {},
                Err(_) => {
                    ret = Err(());
                }
            }
        },
        Err(e) => {
            ret = Err(());
            error!(logger, "Failed to send a message to zfs manager: {}", e);
        }
    }
    ret
}

impl Handler<ExecuteTask> for TaskManager {
    type Result = ResponseFuture<Result<(), String>>;

    fn handle(&mut self, msg: ExecuteTask, _ctx: &mut Context<Self>) -> Self::Result {
        info!(self.logger, "Processing task \"{}\"", msg.0.as_str());
        let zfs_addr = self.zfs_manager.clone();
        let maybe_task = self.tasks.get(msg.0.as_str()).cloned();
        let logger = self.logger.new(o!("task" => msg.0.clone()));
        Box::pin(async move {
            process_task(msg.0, maybe_task, logger, zfs_addr).await
        })
    }
}
fn get_snapshot_name() -> String {
    let date = Utc::today().format("%Y%m%d");
    format!("gazpacho-{}", date)
}
