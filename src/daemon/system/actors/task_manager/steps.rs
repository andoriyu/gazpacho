use crate::daemon::config::Task;
use crate::daemon::strategy::Strategy;
use crate::daemon::system::actors::destination_manager::DestinationManager;
use crate::daemon::system::actors::task_manager::TaskManager;
use crate::daemon::system::actors::zfs_manager::ZfsManager;
use crate::daemon::system::messages::task_manager::{CompletionState, LogTask, RowId};
use crate::daemon::system::messages::zfs_manager::{GetDatasetsForTask, MakeSnapshots};
use actix::{run, Addr, MailboxError, SystemService};
use chrono::Utc;
use futures::stream::FuturesUnordered;
use rusqlite::Error as SqlError;
use slog::{debug, warn, Logger};
use std::fmt::{Display, Error as FmtError, Formatter};
use std::path::PathBuf;
use tokio::sync::Semaphore;

#[derive(Debug)]
pub struct DatasetError {
    pub dataset: PathBuf,
    pub error: String,
}
pub enum StepError {
    TaskNotFound(String),
    MailboxError(MailboxError),
    SqlError(SqlError),
    ZfsError(String),
    PartialErrors(Vec<DatasetError>),
}

impl From<MailboxError> for StepError {
    fn from(src: MailboxError) -> StepError {
        StepError::MailboxError(src)
    }
}

impl From<SqlError> for StepError {
    fn from(src: SqlError) -> StepError {
        StepError::SqlError(src)
    }
}

impl Display for StepError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            StepError::TaskNotFound(ref task_name) => write!(f, "Task {} not found", task_name),
            StepError::MailboxError(ref source) => write!(f, "MailboxError: {}", source),
            StepError::SqlError(ref source) => write!(f, "SqlError: {}", source),
            StepError::ZfsError(ref source) => write!(f, "ZfsError: {}", source),
            StepError::PartialErrors(ref source) => write!(f, "PartialErrors: {:?}", source),
        }
    }
}

pub(super) async fn process_task_step_wrapper(
    task_name: String,
    maybe_task: Option<Task>,
    logger: Logger,
    zfs_addr: Addr<ZfsManager>,
    self_addr: Addr<TaskManager>,
) -> Result<(), StepError> {
    let task = maybe_task.ok_or_else(|| StepError::TaskNotFound(task_name.clone()))?;
    let run_id = task_log_progress(self_addr.clone(), LogTask::Started(task_name.clone())).await?;
    let result =
        process_task_step(task_name, task, logger, zfs_addr, self_addr.clone(), run_id).await;
    let completion_state = match result {
        Ok(_) => CompletionState::Completed,
        Err(StepError::PartialErrors(_)) => CompletionState::CompletedWithErrors,
        _ => CompletionState::Failed,
    };
    let log_msg = LogTask::Completed(run_id, completion_state);
    let _ = task_log_progress(self_addr, log_msg).await?;
    result
}

async fn process_task_step(
    task_name: String,
    task: Task,
    logger: Logger,
    zfs_addr: Addr<ZfsManager>,
    self_addr: Addr<TaskManager>,
    run_id: RowId,
) -> Result<(), StepError> {
    let datasets = get_datasets_for_task(&task, &zfs_addr, &logger).await?;
    let mut has_errors = false;
    let snapshot_name = get_snapshot_name();
    let _ = make_snapshots(datasets.clone(), snapshot_name.clone(), &zfs_addr).await?;

    let dst_manager = DestinationManager::from_registry();
    let semaphore = Semaphore::new(task.parallelism as usize);
    unimplemented!();
}

async fn task_log_progress(self_addr: Addr<TaskManager>, msg: LogTask) -> Result<RowId, StepError> {
    let res = self_addr.send(msg).await??;
    Ok(res)
}

async fn get_datasets_for_task(
    task: &Task,
    zfs_addr: &Addr<ZfsManager>,
    logger: &Logger,
) -> Result<Vec<PathBuf>, StepError> {
    let (zpool, filter) = task.strategy.get_zpool_and_filter();
    let req = GetDatasetsForTask::new(zpool, filter);
    let res = zfs_addr.send(req).await?;
    if res.is_empty() {
        warn!(logger, "Got no datasets to work with")
    } else {
        debug!(logger, "Got {} datasets to work with", res.len());
    }
    Ok(res)
}

async fn make_snapshots(
    datasets: Vec<PathBuf>,
    snapshot_name: String,
    zfs_addr: &Addr<ZfsManager>,
) -> Result<(), StepError> {
    let req = MakeSnapshots::new(datasets, snapshot_name);
    zfs_addr
        .send(req)
        .await?
        .map_err(|e| StepError::ZfsError(e))
}

fn get_snapshot_name() -> String {
    let date = Utc::today().format("%Y%m%d");
    format!("gazpacho-{}", date)
}
