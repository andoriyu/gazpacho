use crate::daemon::system::messages::task_manager::{CompletionState, RowId};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};

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

pub fn query_super_source_for_step(
    conn: &Connection,
    dataset: &str,
    pool: &str,
    task: &str,
) -> Result<Option<String>, rusqlite::Error> {
    let mut stmt = conn.prepare(
        "SELECT source_super FROM step_log WHERE dataset = ?1 AND pool = ?2 AND task = ?3 and state = ?4 ORDER BY completed_at DESC",
    )?;
    let state = format!("{:?}", CompletionState::Completed);
    let last: Option<String> = stmt
        .query_row(&[dataset, pool, task, &state], |row| row.get(0))
        .optional()?;
    Ok(last)
}

pub fn insert_step_log(
    conn: &Connection,
    run_id: RowId,
    task_name: &str,
    pool: &str,
    dataset: &str,
    snapshot: &str,
    source: &Option<String>,
    source_super: &Option<String>,
    timestamp: DateTime<Utc>,
) -> Result<RowId, rusqlite::Error> {
    let state = format!("{:?}", CompletionState::Pending);
    let now = timestamp.to_rfc3339();
    let mut stmt = conn.prepare("INSERT INTO step_log (run_id, state, task, pool, dataset, snapshot, source, source_super, started_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)")?;
    let row_id = stmt.insert(params![
        run_id,
        state,
        task_name,
        pool,
        dataset,
        snapshot,
        source,
        source_super,
        now,
    ])?;
    Ok(row_id)
}

pub fn update_step_log(
    conn: &Connection,
    row_id: RowId,
    state: CompletionState,
    timestamp: DateTime<Utc>,
) -> Result<RowId, rusqlite::Error> {
    let now = timestamp.to_rfc3339();
    let state = format!("{:?}", state);
    let mut stmt =
        conn.prepare("UPDATE step_log SET state = ?1, completed_at = ?2 WHERE id = ?3")?;
    stmt.execute(params![state, now, row_id])?;

    Ok(row_id)
}
