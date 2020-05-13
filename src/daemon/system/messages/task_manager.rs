use crate::daemon::config::Task;
use crate::daemon::system::actors::task_manager::StepError;
use actix::Message;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;

pub type StepLogMessage = TimestampedMessage<StepLog>;
pub type TaskLogMessage = TimestampedMessage<TaskLog>;
pub type UpdateResetCountsMessage = TimestampedMessage<UpdateResetCounts>;
pub type RowId = i64;

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

impl Display for CompletionState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub struct TimestampedMessage<T> {
    pub timestamp: DateTime<Utc>,
    pub payload: T,
}

/// Various types of task level log events
pub enum TaskLog {
    /// Task started
    Started(String),
    /// Task finished
    Completed(RowId, CompletionState),
}

impl Message for TimestampedMessage<TaskLog> {
    type Result = Result<RowId, rusqlite::Error>;
}

impl TimestampedMessage<TaskLog> {
    /// Create a new message for task-started event.
    pub fn started(name: String, timestamp: DateTime<Utc>) -> Self {
        Self {
            payload: TaskLog::Started(name),
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
            payload: TaskLog::Completed(row_id, state),
            timestamp,
        }
    }

    /// Create a new message for task-finished event with an implicit timestamp.
    pub fn completed_now(row_id: RowId, state: CompletionState) -> Self {
        Self::completed(row_id, state, Utc::now())
    }
}

pub enum StepLog {
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

impl TimestampedMessage<StepLog> {
    pub fn started(
        run_id: RowId,
        task: String,
        pool: String,
        dataset: PathBuf,
        snapshot: String,
        source: Option<PathBuf>,
        timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            timestamp,
            payload: StepLog::Started {
                run_id,
                task,
                pool,
                dataset,
                snapshot,
                source,
            },
        }
    }
    pub fn started_now(
        run_id: RowId,
        task: String,
        pool: String,
        dataset: PathBuf,
        snapshot: String,
        source: Option<PathBuf>,
    ) -> Self {
        Self::started(run_id, task, pool, dataset, snapshot, source, Utc::now())
    }

    pub fn completed(row_id: RowId, state: CompletionState, timestamp: DateTime<Utc>) -> Self {
        Self {
            timestamp,
            payload: StepLog::Completed { row_id, state },
        }
    }

    pub fn completed_now(row_id: RowId, state: CompletionState) -> Self {
        Self::completed(row_id, state, Utc::now())
    }
}

impl Message for TimestampedMessage<StepLog> {
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

pub struct UpdateResetCounts {
    pub task: String,
    pub reset: bool,
}

impl Message for TimestampedMessage<UpdateResetCounts> {
    type Result = Result<(), rusqlite::Error>;
}

impl TimestampedMessage<UpdateResetCounts> {
    pub fn new(task: String, reset: bool, timestamp: DateTime<Utc>) -> Self {
        Self {
            timestamp,
            payload: UpdateResetCounts { task, reset },
        }
    }
}
