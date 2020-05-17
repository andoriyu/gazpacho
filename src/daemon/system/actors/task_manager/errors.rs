use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum RepositoryError {
    InsertTaskLogError(InsertTaskLogError),
    UpdateTaskLogError(UpdateTaskLogError),
}

#[derive(Debug)]
pub enum InsertTaskLogError {
    PreparedStatementError(rusqlite::Error),
    InsertError(rusqlite::Error),
}

#[derive(Debug)]
pub enum UpdateTaskLogError {
    PreparedStatementError(rusqlite::Error),
    UpdateError(rusqlite::Error),
}

impl Display for InsertTaskLogError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            InsertTaskLogError::PreparedStatementError(source) => {
                write!(f, "Failed to prepare task log insert statement: {}", source)
            }
            InsertTaskLogError::InsertError(source) => {
                write!(f, "Failed to insert task log row: {}", source)
            }
        }
    }
}
impl Display for UpdateTaskLogError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateTaskLogError::PreparedStatementError(source) => {
                write!(f, "Failed to prepare task log update statement: {}", source)
            }
            UpdateTaskLogError::UpdateError(source) => {
                write!(f, "Failed to update task log row: {}", source)
            }
        }
    }
}

impl Display for RepositoryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RepositoryError::InsertTaskLogError(src) => write!(f, "Repository error: {}", src),
            RepositoryError::UpdateTaskLogError(src) => write!(f, "Repository error: {}", src),
        }
    }
}

impl From<InsertTaskLogError> for RepositoryError {
    fn from(src: InsertTaskLogError) -> Self {
        RepositoryError::InsertTaskLogError(src)
    }
}
impl From<UpdateTaskLogError> for RepositoryError {
    fn from(src: UpdateTaskLogError) -> Self {
        RepositoryError::UpdateTaskLogError(src)
    }
}
