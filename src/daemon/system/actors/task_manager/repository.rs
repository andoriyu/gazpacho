use super::messages::{CompletionState, RowId};
use crate::daemon::system::actors::task_manager::errors::{InsertTaskLogError, UpdateTaskLogError};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::HashMap;
use std::path::PathBuf;

pub fn insert_task_log(
    conn: &Connection,
    task_name: &str,
    timestamp: DateTime<Utc>,
) -> Result<RowId, InsertTaskLogError> {
    let state = CompletionState::Pending.to_string();
    let time_of_event = timestamp.to_rfc3339();
    let mut stmt = conn
        .prepare("INSERT INTO task_log (task, started_at, state) VALUES (?1, ?2, ?3)")
        .map_err(|e| InsertTaskLogError::PreparedStatementError(e))?;
    let row_id = stmt
        .insert(&[task_name, &time_of_event, &state])
        .map_err(|e| InsertTaskLogError::InsertError(e))?;
    Ok(row_id)
}

pub fn update_task_log_state(
    conn: &Connection,
    row_id: RowId,
    state: CompletionState,
    timestamp: DateTime<Utc>,
) -> Result<RowId, UpdateTaskLogError> {
    let state = state.to_string();
    let time_of_event = timestamp.to_rfc3339();

    let mut stmt = conn
        .prepare("UPDATE task_log SET completed_at = ?1, state = ?2 WHERE id = ?3")
        .map_err(|e| UpdateTaskLogError::PreparedStatementError(e))?;
    stmt.execute(params![time_of_event, state, row_id])
        .map_err(|e| UpdateTaskLogError::UpdateError(e))?;
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
    let state = CompletionState::Completed.to_string();
    let last: Option<String> = stmt
        .query_row(&[dataset, pool, task, &state], |row| row.get(0))
        .optional()?
        .flatten();
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
    let state = CompletionState::Pending.to_string();
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

pub fn get_count_and_date_of_last_reset(
    conn: &Connection,
    task_name: &str,
) -> Result<Option<(i64, DateTime<Utc>)>, rusqlite::Error> {
    let mut stmt = conn.prepare("SELECT count, reset_at FROM reset_count WHERE task = ?1")?;

    stmt.query_row(&[task_name], |row| {
        let count: i64 = row.get(0)?;
        let date: String = row.get(1)?;
        let date = DateTime::parse_from_rfc3339(&date)
            .expect("Failed to parser timestamp")
            .into();
        Ok((count, date))
    })
    .optional()
}

pub fn get_sources(
    conn: &Connection,
    pool: &str,
    datasets: &[PathBuf],
    task_name: &str,
) -> Result<HashMap<PathBuf, PathBuf>, rusqlite::Error> {
    let mut last_snapshot_stms = conn.prepare(
        "SELECT snapshot FROM step_log WHERE dataset = ?1 AND pool = ?2 AND task = ?3 AND state = ?4 ORDER BY completed_at DESC",
    )?;

    let mut ret = HashMap::with_capacity(datasets.len());
    let state = CompletionState::Completed.to_string();

    for dataset in datasets {
        let dataset_as_str = dataset.to_string_lossy().to_string();
        let snapshot = last_snapshot_stms
            .query_row(&[&dataset_as_str, pool, task_name, &state], |row| {
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

pub fn check_if_readonly(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute("PRAGMA user_version = 0;", params![])
        .map(|_| ())
}

pub fn update_reset_counts(
    conn: &Connection,
    task_name: &str,
    reset_at: Option<DateTime<Utc>>,
) -> Result<(), rusqlite::Error> {
    if let Some((count, last_reset_at)) = get_count_and_date_of_last_reset(conn, task_name)? {
        let mut stms =
            conn.prepare("UPDATE reset_count SET count = ?1, reset_at = ?2 WHERE task = ?3")?;
        let (new_count, new_reset_at) = if let Some(reset_at) = reset_at {
            (0, reset_at)
        } else {
            (count + 1, last_reset_at)
        };

        stms.execute(params![new_count, &new_reset_at.to_rfc3339(), task_name])?;
    } else {
        let reset_at = reset_at
            .expect("no reset_at, but the record is new")
            .to_rfc3339();
        let mut stms =
            conn.prepare("INSERT INTO reset_count (count, reset_at, task) VALUES (?1, ?2, ?3);")?;

        stms.execute(params![0, &reset_at, task_name])?;
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use std::error::Error;

    static TASK_NAME: &str = "test-task";

    fn get_connection() -> Connection {
        let mut conn = Connection::open_in_memory().unwrap();
        crate::db::task_manager::runner().run(&mut conn).unwrap();
        conn
    }

    #[test]
    fn get_last_reset_info() -> Result<(), Box<dyn Error>> {
        let conn = get_connection();
        let last_reset = get_count_and_date_of_last_reset(&conn, TASK_NAME)?;
        assert!(last_reset.is_none());

        {
            let last_reset_time = Utc::now();
            update_reset_counts(&conn, TASK_NAME, Some(last_reset_time.clone()))?;

            let (count, timestamp) = get_count_and_date_of_last_reset(&conn, TASK_NAME)?.unwrap();
            assert_eq!(0, count);
            assert_eq!(last_reset_time, timestamp);
        }
        {
            let (_, old_timestamp) = get_count_and_date_of_last_reset(&conn, TASK_NAME)?.unwrap();
            update_reset_counts(&conn, TASK_NAME, None)?;

            let (count, timestamp) = get_count_and_date_of_last_reset(&conn, TASK_NAME)?.unwrap();
            assert_eq!(1, count);
            assert_eq!(old_timestamp, timestamp);
        }
        {
            let (_, old_timestamp) = get_count_and_date_of_last_reset(&conn, TASK_NAME)?.unwrap();
            update_reset_counts(&conn, TASK_NAME, None)?;

            let (count, timestamp) = get_count_and_date_of_last_reset(&conn, TASK_NAME)?.unwrap();
            assert_eq!(2, count);
            assert_eq!(old_timestamp, timestamp);
        }
        {
            let last_reset_time = Utc::now();
            update_reset_counts(&conn, TASK_NAME, Some(last_reset_time.clone()))?;

            let (count, timestamp) = get_count_and_date_of_last_reset(&conn, TASK_NAME)?.unwrap();
            assert_eq!(0, count);
            assert_eq!(last_reset_time, timestamp);
        }
        Ok(())
    }
}
