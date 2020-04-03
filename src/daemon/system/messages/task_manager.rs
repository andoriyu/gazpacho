use crate::daemon::config::{Configuration, Task};
use crate::daemon::system::actors::task_manager::steps::StepError;
use actix::Message;
use std::collections::HashMap;
use std::path::PathBuf;

pub type RowId = i64;
#[derive(Debug)]
pub struct NewConfiguration(pub Configuration);
impl Message for NewConfiguration {
    type Result = ();
}

pub struct ExecuteTask(pub String);

impl Message for ExecuteTask {
    type Result = Result<(), StepError>;
}

#[derive(Debug, Eq, PartialEq)]
pub enum CompletionState {
    Pending,
    Completed,
    CompletedWithErrors,
    Failed,
}
pub enum LogTask {
    Started(String),
    Completed(RowId, CompletionState),
}
impl Message for LogTask {
    type Result = Result<RowId, rusqlite::Error>;
}

pub enum LogStep {
    Started {
        run_id: RowId,
        task: String,
        pool: String,
        dataset: PathBuf,
        snapshot: String,
        source: Option<PathBuf>,
    },
    Completed {
        row_id: RowId,
        state: CompletionState,
    },
}

impl LogStep {
    pub fn started(
        run_id: RowId,
        task: String,
        pool: String,
        dataset: PathBuf,
        snapshot: String,
        source: Option<PathBuf>,
    ) -> Self {
        LogStep::Started {
            run_id,
            task,
            pool,
            dataset,
            snapshot,
            source,
        }
    }
    pub fn completed(row_id: RowId, state: CompletionState) -> Self {
        LogStep::Completed { row_id, state }
    }
}

impl Message for LogStep {
    type Result = Result<RowId, rusqlite::Error>;
}

pub struct NeedsReset {
    pub task_name: String,
    pub task: Task,
}

impl NeedsReset {
    pub fn new(task_name: String, task: Task) -> Self {
        NeedsReset { task_name, task }
    }
}

impl Message for NeedsReset {
    type Result = Result<bool, rusqlite::Error>;
}

pub struct ResetTimesSinceReset(String);
impl Message for ResetTimesSinceReset {
    type Result = Result<(), rusqlite::Error>;
}

pub struct IncrementTimesSinceReset(String);
impl Message for IncrementTimesSinceReset {
    type Result = Result<(), rusqlite::Error>;
}

pub struct GetSources {
    pub task_name: String,
    pub task: Task,
    pub datasets: Vec<PathBuf>,
}

impl GetSources {
    pub fn new(task_name: String, task: Task, datasets: Vec<PathBuf>) -> Self {
        GetSources {
            task_name,
            task,
            datasets,
        }
    }
}

impl Message for GetSources {
    type Result = Result<HashMap<PathBuf, PathBuf>, rusqlite::Error>;
}
