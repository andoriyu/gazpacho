use crate::daemon::system::messages::task_manager::{CompletionState, RowId};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};

pub fn insert_task_log(
    conn: &Connection,
    task_name: String,
    timestamp: DateTime<Utc>,
) -> Result<RowId, rusqlite::Error> {
    let state = format!("{:?}", CompletionState::Pending);
    let time_of_event = timestamp.to_rfc3339();
    let mut stmt =
        conn.prepare("INSERT INTO task_log (task, started_at, state) VALUES (?1, ?2, ?3)")?;
    let row_id = stmt.insert(&[task_name, time_of_event, state])?;
    Ok(row_id)
}

pub fn update_task_log_state(
    conn: &Connection,
    row_id: RowId,
    state: CompletionState,
    timestamp: DateTime<Utc>,
) -> Result<RowId, rusqlite::Error> {
    let state = format!("{:?}", state);
    let time_of_event = timestamp.to_rfc3339();

    let mut stmt =
        conn.prepare("UPDATE task_log SET completed_at = ?1, state = ?2  WHERE id = ?3")?;
    stmt.execute(params![time_of_event, state, row_id])?;

    Ok(row_id)
}
