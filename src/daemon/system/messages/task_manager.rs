use crate::daemon::config::{Configuration, Task};
use crate::daemon::system::actors::task_manager::StepError;
use actix::Message;
use chrono::{DateTime, Utc};
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

/// A message representing log event on task level.
pub struct TaskLogMessage {
    /// Type of the event
    pub event: TaskLog,
    /// Time of the event
    pub timestamp: DateTime<Utc>,
}

/// Various types of task level log events
pub enum TaskLog {
    /// Task started
    Started(String),
    /// Task finished
    Completed(RowId, CompletionState),
}

impl Message for TaskLogMessage {
    type Result = Result<RowId, rusqlite::Error>;
}

impl TaskLogMessage {
    /// Create a new message for task-started event.
    pub fn started(name: String, timestamp: DateTime<Utc>) -> Self {
        Self {
            event: TaskLog::Started(name),
            timestamp,
        }
    }
    /// Create a new message for task-started event with an implicit timestamp.
    pub fn started_now(name: String) -> Self {
        Self::started(name, Utc::now())
    }

    /// Create a new message for task-finished event.
    pub fn completed(row_id: RowId, state: CompletionState, timestamp: DateTime<Utc>) -> Self {
        Self {
            event: TaskLog::Completed(row_id, state),
            timestamp,
        }
    }

    /// Create a new message for task-finished event with an implicit timestamp.
    pub fn completed_now(row_id: RowId, state: CompletionState) -> Self {
        Self::completed(row_id, state, Utc::now())
    }
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
