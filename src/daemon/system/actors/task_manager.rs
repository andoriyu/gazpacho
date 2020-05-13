use crate::daemon::config::Task;
use crate::daemon::logging::GlobalLogger;
use crate::daemon::strategy::Strategy;
use crate::daemon::system::actors::destination_manager::DestinationManager;
pub use crate::daemon::system::actors::task_manager::steps::StepError;
use crate::daemon::system::actors::zfs_manager::ZfsManager;
use crate::daemon::system::messages::destination_manager::NewDestinations;
use crate::daemon::system::messages::task_manager::{
    ExecuteTask, GetSources, NeedsReset, RowId, StepLog, StepLogMessage, TaskLog, TaskLogMessage,
    UpdateResetCountsMessage,
};
use crate::daemon::system::shutdown;
use crate::daemon::CURRENT_CONFIGURATION;
use actix::fut::wrap_future;
use actix::{
    Actor, ActorFuture, Addr, AsyncContext, Context, Handler, ResponseFuture, SpawnHandle,
    Supervised, SyncArbiter, SystemService,
};
use chrono::Utc;
use rusqlite::Connection;
use slog::Logger;
use slog::{debug, error, o, warn};
use std::collections::HashMap;
use std::path::PathBuf;

mod repository;
mod steps;

pub struct TaskManager {
    logger: Logger,
    db: Option<Connection>,
    tasks: HashMap<String, Task>,
    zfs_manager: Addr<ZfsManager>,
    active_runners: HashMap<String, SpawnHandle>,
}
impl Default for TaskManager {
    fn default() -> Self {
        let logger =
            GlobalLogger::get().new(o!("module" => module_path!(), "actor" => "TaskRegistry"));
        let conf = CURRENT_CONFIGURATION.get().unwrap();
        let db_path = conf.daemon.database.as_path();
        debug!(logger, "Trying to open database at '{}'", db_path.display());
        let mut db = match Connection::open(db_path) {
            Ok(conn) => conn,
            Err(e) => {
                error!(logger, "Failed to open database: {}", e);
                shutdown();
                // Service is going to shutdown, so it doesn't matter
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
        if let Err(e) = repository::check_if_readonly(&db) {
            error!(logger, "Possibly a corrupted database: {}", e);
            shutdown();
        }
        let zfs_manager = SyncArbiter::start(conf.parallelism as usize, ZfsManager::default);
        TaskManager {
            logger,
            db: Some(db),
            tasks: HashMap::new(),
            zfs_manager,
            active_runners: HashMap::new(),
        }
    }
}

impl Actor for TaskManager {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        debug!(self.logger, "Actor started");

        if let Some(configuration) = CURRENT_CONFIGURATION.get() {
            let tasks: HashMap<String, Task> = configuration
                .tasks
                .clone()
                .into_iter()
                .filter(|(name, task)| {
                    let dst = task.destination.as_str();
                    if configuration.destinations.contains_key(dst) {
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

            let dsts = configuration
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

impl Handler<ExecuteTask> for TaskManager {
    type Result = ResponseFuture<Result<(), StepError>>;

    fn handle(&mut self, msg: ExecuteTask, ctx: &mut Context<Self>) -> Self::Result {
        let name = &msg.0;
        let key = name.clone();
        let zfs_addr = self.zfs_manager.clone();
        let maybe_task = self.tasks.get(msg.0.as_str()).cloned();
        let logger = self.logger.new(o!("task" => msg.0.clone()));
        let self_addr = ctx.address();
        if self.active_runners.contains_key(&key) {
            let name = name.clone();
            warn!(logger, "Task \"{}\" is already running!", &key);
            return Box::pin(async move { Err(StepError::AlreadyRunning(name)) });
        }
        let (tx, rx) = futures::channel::oneshot::channel();
        let runner = steps::process_task_step_wrapper(
            name.clone(),
            maybe_task,
            logger.clone(),
            zfs_addr,
            self_addr,
        );
        let runner_wrapped = wrap_future(async move {
            let ret = runner.await;
            let _ = tx.send(ret);
            key.clone()
        })
        .map(|key, actor: &mut TaskManager, _ctx| {
            actor.active_runners.remove(&key);
        });

        let handler = ctx.spawn(runner_wrapped);
        self.active_runners.insert(name.clone(), handler);
        Box::pin(async move {
            if let Ok(ret) = rx.await {
                ret
            } else {
                Ok(()) // It's been canceled, not need to worry about it.
            }
        })
    }
}

impl Handler<TaskLogMessage> for TaskManager {
    type Result = Result<RowId, rusqlite::Error>;

    fn handle(&mut self, msg: TaskLogMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let conn = self.db.as_ref().expect("Failed to acquire connection");
        match msg.payload {
            TaskLog::Started(task_name) => {
                repository::insert_task_log(conn, &task_name, msg.timestamp)
            }
            TaskLog::Completed(row_id, completion_state) => {
                repository::update_task_log_state(conn, row_id, completion_state, msg.timestamp)
            }
        }
    }
}

impl Handler<StepLogMessage> for TaskManager {
    type Result = Result<RowId, rusqlite::Error>;

    fn handle(&mut self, msg: StepLogMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let conn = self.db.as_ref().unwrap();
        match msg.payload {
            StepLog::Started {
                run_id,
                task,
                pool,
                dataset,
                snapshot,
                source,
            } => {
                let dataset = dataset.to_string_lossy().to_string();
                let source = source.map(|e| e.to_string_lossy().to_string());
                let source_super = if source.is_some() {
                    repository::query_super_source_for_step(conn, &dataset, &pool, &task)?
                        .or_else(|| source.clone())
                } else {
                    None
                };
                repository::insert_step_log(
                    conn,
                    run_id,
                    &task,
                    &pool,
                    &dataset,
                    &snapshot,
                    &source,
                    &source_super,
                    msg.timestamp,
                )
            }
            StepLog::Completed { row_id, state } => {
                repository::update_step_log(conn, row_id, state, msg.timestamp)
            }
        }
    }
}

impl Handler<NeedsReset> for TaskManager {
    type Result = Result<bool, rusqlite::Error>;

    fn handle(&mut self, msg: NeedsReset, _ctx: &mut Context<Self>) -> Self::Result {
        let conn = self.db.as_ref().unwrap();
        match msg.task.strategy {
            Strategy::Full(_) => Ok(true),
            Strategy::Incremental(stg) => {
                let current = repository::get_count_and_date_of_last_reset(&conn, &msg.task_name)?;
                debug!(self.logger, "Reset information: {:?}", current; "task" => msg.task_name.clone());
                let needs_reset = stg.check_if_needs_reset(current, Utc::now());
                Ok(needs_reset)
            }
        }
    }
}

impl Handler<GetSources> for TaskManager {
    type Result = Result<HashMap<PathBuf, PathBuf>, rusqlite::Error>;

    fn handle(&mut self, msg: GetSources, _ctx: &mut Context<Self>) -> Self::Result {
        let conn = self.db.as_ref().unwrap();
        let (pool, _) = msg.task.strategy.get_zpool_and_filter();
        repository::get_sources(&conn, &pool, &msg.datasets, &msg.task_name)
    }
}

impl Handler<UpdateResetCountsMessage> for TaskManager {
    type Result = Result<(), rusqlite::Error>;

    fn handle(&mut self, msg: UpdateResetCountsMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let conn = self.db.as_ref().unwrap();
        let reset_at = if msg.payload.reset {
            Some(msg.timestamp.clone())
        } else {
            None
        };
        repository::update_reset_counts(conn, &msg.payload.task, reset_at)
    }
}
/*
struct MyActor(u8);

impl Actor for MyActor {
    type Context = Context<Self>;
}


#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    let side_stuff = Arc::new(Mutex::new(1));

    let address = MyActor::create(|ctx| {
        let actor = MyActor(0);

        let fut = async {
            actix::clock::delay_for(Duration::from_secs(1)).await;
            1
        };

        let side_clone = side_stuff.clone();

        fut.into_actor(&actor)
            .then(move |new_state, mut act, ctx| {
                act.0 = new_state;
                *side_clone.lock().unwrap() += 1;

                assert_eq!(1, act.0);

                actix::fut::ready(())
            })
            .wait(ctx);

        actor
    });

    actix::clock::delay_for(Duration::from_secs(2)).await;

    assert_eq!(2, *side_stuff.lock().unwrap());

    Ok(())
}
 */
