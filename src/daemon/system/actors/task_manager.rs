use crate::daemon::config::Task;
use crate::daemon::logging::GlobalLogger;
use crate::daemon::strategy::Strategy;
use crate::daemon::system::actors::destination_manager::DestinationManager;
pub use crate::daemon::system::actors::task_manager::steps::StepError;
use crate::daemon::system::actors::zfs_manager::ZfsManager;
use crate::daemon::system::messages::destination_manager::NewDestinations;
use crate::daemon::system::messages::task_manager::{
    CompletionState, ExecuteTask, GetSources, NeedsReset, NewConfiguration, RowId, StepLog,
    StepLogMessage, TaskLog, TaskLogMessage,
};
use crate::daemon::system::shutdown;
use crate::daemon::STARTUP_CONFIGURATION;
use actix::{
    Actor, Addr, AsyncContext, Context, Handler, ResponseFuture, Supervised, SyncArbiter,
    SystemService,
};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{Connection, OptionalExtension};
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
}
impl Default for TaskManager {
    fn default() -> Self {
        let logger =
            GlobalLogger::get().new(o!("module" => module_path!(), "actor" => "TaskRegistry"));
        let conf = STARTUP_CONFIGURATION.get().unwrap();
        let db_path = conf.daemon.database.as_path();
        debug!(logger, "Trying to open database at '{}'", db_path.display());
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

impl Handler<ExecuteTask> for TaskManager {
    type Result = ResponseFuture<Result<(), StepError>>;

    fn handle(&mut self, msg: ExecuteTask, ctx: &mut Context<Self>) -> Self::Result {
        let zfs_addr = self.zfs_manager.clone();
        let maybe_task = self.tasks.get(msg.0.as_str()).cloned();
        let logger = self.logger.new(o!("task" => msg.0.clone()));
        let self_addr = ctx.address();
        Box::pin(async move {
            steps::process_task_step_wrapper(msg.0, maybe_task, logger, zfs_addr, self_addr).await
        })
    }
}

impl Handler<TaskLogMessage> for TaskManager {
    type Result = Result<RowId, rusqlite::Error>;

    fn handle(&mut self, msg: TaskLogMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let conn = self.db.as_ref().expect("Failed to acquire connection");
        match msg.event {
            TaskLog::Started(task_name) => {
                repository::insert_task_log(conn, task_name, msg.timestamp)
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
        match msg.event {
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
            Strategy::Full(_) => Ok(false),
            Strategy::Incremental(stg) => {
                let mut needs_reset = true;
                let current = {
                    let mut stmt =
                        conn.prepare("SELECT count, reset_at FROM reset_count WHERE task = ?1")?;

                    stmt.query_row(&[msg.task_name], |row| {
                        let count: i64 = row.get(0)?;
                        let date: String = row.get(1)?;
                        Ok((count, date))
                    })
                    .optional()?
                };
                if let Some((count, last_date)) = current {
                    if let Some(max_times_since_last_reset) = stg.runs_before_reset {
                        if count < max_times_since_last_reset {
                            needs_reset = false;
                        }
                    }
                    if let Some(max_days_since_last_reset) = stg.days_before_reset {
                        let date: DateTime<Utc> =
                            DateTime::parse_from_rfc3339(&last_date).unwrap().into();
                        let today = Utc::now();
                        let time_since = today - date;
                        let max_time_since = Duration::days(max_days_since_last_reset);
                        if time_since < max_time_since {
                            needs_reset = false
                        }
                    }
                }
                Ok(needs_reset)
            }
        }
    }
}

impl Handler<GetSources> for TaskManager {
    type Result = Result<HashMap<PathBuf, PathBuf>, rusqlite::Error>;

    fn handle(&mut self, msg: GetSources, _ctx: &mut Context<Self>) -> Self::Result {
        let mut ret = HashMap::with_capacity(msg.datasets.len());
        let conn = self.db.as_ref().unwrap();

        let (pool, _) = msg.task.strategy.get_zpool_and_filter();
        let state = format!("{:?}", CompletionState::Completed);
        let mut last_snapshot_stms = conn.prepare(
            "SELECT snapshot FROM step_log WHERE dataset = ?1 AND pool = ?2 AND task ?3 AND state = ?4 ORDER BY completed_at DESC",
        ).map_err(|e| {
            error!(self.logger, "Failed to prepare last snapshot statement: {}", e);
            e
        })?;

        dbg!("hit");

        for dataset in msg.datasets {
            let dataset_as_str = dataset.to_string_lossy().to_string();
            let snapshot = last_snapshot_stms
                .query_row(&[&dataset_as_str, &pool, &msg.task_name, &state], |row| {
                    let snapshot: String = row.get(0)?;
                    Ok(snapshot)
                })
                .optional()?;
            if let Some(snap) = snapshot {
                let snapshot_full = format!("{}@{}", &dataset_as_str, snap).into();
                ret.insert(dataset.clone(), snapshot_full);
            }
        }
        Ok(ret)
    }
}
