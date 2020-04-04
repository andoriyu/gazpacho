use crate::daemon::config::Task;
use crate::daemon::system::actors::destination_manager::DestinationManager;
use crate::daemon::system::actors::task_manager::TaskManager;
use crate::daemon::system::actors::zfs_manager::ZfsManager;
use crate::daemon::system::messages::destination_manager::SaveFromPipe;
use crate::daemon::system::messages::task_manager::{
    CompletionState, GetSources, LogStep, LogTask, NeedsReset, RowId,
};
use crate::daemon::system::messages::zfs_manager::{
    GetDatasetsForTask, MakeSnapshots, SendSnapshotToPipe,
};
use actix::{Addr, MailboxError, SystemService};
use chrono::Utc;
use filedescriptor::Pipe;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use rusqlite::Error as SqlError;
use slog::{debug, error, info, o, warn, Logger};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Error as FmtError, Formatter};
use std::path::PathBuf;
use tokio::sync::Semaphore;

#[derive(Debug)]
pub struct DatasetError {
    pub dataset: PathBuf,
    pub error: DatasetErrorKind,
}

#[derive(Debug)]
pub enum DatasetErrorKind {
    MailboxError(MailboxError),
    SqlError(SqlError),
    PipeError(String),
    SendError(SendError),
    Other(String),
}

impl DatasetError {
    pub fn new(dataset: PathBuf, error: DatasetErrorKind) -> Self {
        DatasetError { dataset, error }
    }
    pub fn other(dataset: PathBuf, error: String) -> Self {
        DatasetError {
            dataset,
            error: DatasetErrorKind::Other(error),
        }
    }
}

impl Display for DatasetErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self {
            DatasetErrorKind::PipeError(e) => write!(f, "{}", e),
            DatasetErrorKind::MailboxError(e) => write!(f, "{}", e),
            DatasetErrorKind::SendError(e) => write!(f, "{}", e),
            DatasetErrorKind::SqlError(e) => write!(f, "{}", e),
            DatasetErrorKind::Other(e) => write!(f, "{}", e),
        }
    }
}

impl Display for DatasetError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        f.debug_struct("DatasetError")
            .field("dataset", &self.dataset)
            .field("error", &self.error.to_string())
            .finish()
    }
}

#[derive(Debug)]
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

impl Error for StepError {}

#[derive(Debug, Default)]
pub struct SendError {
    zfs: Option<String>,
    destination: Option<String>,
}

impl Display for SendError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        write!(f, "{:?}", self)
    }
}

impl Error for SendError {}

pub(super) async fn process_task_step_wrapper(
    task_name: String,
    maybe_task: Option<Task>,
    logger: Logger,
    zfs_addr: Addr<ZfsManager>,
    self_addr: Addr<TaskManager>,
) -> Result<(), StepError> {
    info!(logger, "Processing");
    let task = maybe_task.ok_or_else(|| StepError::TaskNotFound(task_name.clone()))?;
    let run_id = task_log_progress(self_addr.clone(), LogTask::Started(task_name.clone())).await?;
    let result = process_task_step(
        task_name,
        task,
        &logger,
        zfs_addr,
        self_addr.clone(),
        run_id,
    )
    .await;
    let completion_state = match &result {
        Ok(_) => CompletionState::Completed,
        Err(StepError::PartialErrors(e)) => {
            error!(logger, "Executed with following errors: {:?}", e);
            CompletionState::CompletedWithErrors
        }
        _ => CompletionState::Failed,
    };
    let log_msg = LogTask::Completed(run_id, completion_state);
    let _ = task_log_progress(self_addr, log_msg).await?;
    match &result {
        Ok(()) => info!(logger, "Finished"),
        Err(e) => error!(logger, "Finished with errors: {}", e),
    };
    result
}

async fn process_task_step(
    task_name: String,
    task: Task,
    logger: &Logger,
    zfs_addr: Addr<ZfsManager>,
    self_addr: Addr<TaskManager>,
    run_id: RowId,
) -> Result<(), StepError> {
    let datasets = get_datasets_for_task(&task, &zfs_addr, &logger).await?;
    let mut errors = Vec::new();
    let snapshot_name = get_snapshot_name();
    let _ = make_snapshots(datasets.clone(), snapshot_name.clone(), &zfs_addr).await?;
    let needs_reset = check_needs_reset(task_name.clone(), task.clone(), &self_addr).await?;
    let sources = get_sources(
        task_name.clone(),
        task.clone(),
        &self_addr,
        needs_reset,
        datasets.clone(),
    )
    .await?;

    let dst_manager = DestinationManager::from_registry();
    let semaphore = Semaphore::new(task.parallelism as usize);
    let mut steps = datasets
        .iter()
        .map(|dataset| {
            let source = sources.get(dataset).cloned();
            process_dataset(
                &logger,
                &zfs_addr,
                &task,
                &snapshot_name,
                &dst_manager,
                &semaphore,
                dataset.clone(),
                &self_addr,
                run_id,
                task_name.clone(),
                source,
            )
        })
        .collect::<FuturesUnordered<_>>();

    while let Some(result) = steps.next().await {
        if let Err(e) = result {
            errors.push(e);
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(StepError::PartialErrors(errors))
    }
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

async fn check_needs_reset(
    task_name: String,
    task: Task,
    self_addr: &Addr<TaskManager>,
) -> Result<bool, StepError> {
    let ret = self_addr.send(NeedsReset::new(task_name, task)).await??;
    Ok(ret)
}

async fn get_sources(
    task_name: String,
    task: Task,
    self_addr: &Addr<TaskManager>,
    needs_reset: bool,
    datasets: Vec<PathBuf>,
) -> Result<HashMap<PathBuf, PathBuf>, StepError> {
    if needs_reset {
        return Ok(HashMap::new());
    }

    let sources = self_addr
        .send(GetSources::new(task_name, task, datasets))
        .await??;
    Ok(sources)
}

async fn process_dataset(
    logger: &Logger,
    zfs_addr: &Addr<ZfsManager>,
    task: &Task,
    snapshot_name: &String,
    dst_manager: &Addr<DestinationManager>,
    semaphore: &Semaphore,
    dataset: PathBuf,
    self_addr: &Addr<TaskManager>,
    run_id: RowId,
    task_name: String,
    source: Option<PathBuf>,
) -> Result<(), DatasetError> {
    let logger = logger.new(o!("dataset" => dataset.display().to_string()));

    let (pool, _) = task.strategy.get_zpool_and_filter();
    let msg = LogStep::started(
        run_id,
        task_name.clone(),
        pool.clone(),
        dataset.clone(),
        snapshot_name.clone(),
        source.clone(),
    );
    let row_id = step_log_progress(msg, dataset.clone(), &self_addr).await?;
    debug!(logger, "Waiting for a permit work on {}", dataset.display());
    let _permit = semaphore.acquire().await;
    debug!(logger, "Got the permit the work on {}", dataset.display());
    let snapshot = PathBuf::from(format!("{}@{}", dataset.to_string_lossy(), &snapshot_name));
    let pipe = Pipe::new().map_err(|e| {
        DatasetError::new(dataset.clone(), DatasetErrorKind::PipeError(e.to_string()))
    })?;
    let rx = pipe.read.try_clone().map_err(|e| {
        DatasetError::new(dataset.clone(), DatasetErrorKind::PipeError(e.to_string()))
    })?;
    let dst_req = SaveFromPipe::new(
        task.destination.clone(),
        dataset.clone(),
        snapshot.clone(),
        task.compression.clone(),
        rx,
    );
    let dst_res = dst_manager.send(dst_req);
    let zfs_req = SendSnapshotToPipe(snapshot.clone(), source.clone(), pipe);
    let zfs_res = zfs_addr.send(zfs_req);
    let result = futures::try_join!(dst_res, zfs_res)
        .map_err(|e| DatasetError::other(dataset.clone(), e.to_string()))
        .map(|_| ());
    let completion_state = match result {
        Ok(_) => CompletionState::Completed,
        Err(_) => CompletionState::Failed,
    };
    let msg = LogStep::completed(row_id, completion_state);
    step_log_progress(msg, dataset.clone(), &self_addr).await?;
    result
}

async fn step_log_progress(
    msg: LogStep,
    dataset: PathBuf,
    self_addr: &Addr<TaskManager>,
) -> Result<RowId, DatasetError> {
    let ret = self_addr
        .send(msg)
        .await
        .map_err(|e| DatasetError::new(dataset.clone(), DatasetErrorKind::MailboxError(e)))?
        .map_err(|e| DatasetError::new(dataset, DatasetErrorKind::SqlError(e)))?;
    Ok(ret)
}

fn get_snapshot_name() -> String {
    let date = Utc::today().format("%Y%m%d");
    format!("gazpacho-{}", date)
}
