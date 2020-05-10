use crate::daemon::config::Compression;
use crate::daemon::destination::Destination;
use actix::Message;
use chrono::{DateTime, Utc};
use filedescriptor::FileDescriptor;
use std::collections::HashMap;
use std::path::PathBuf;

pub struct NewDestinations(pub HashMap<String, Destination>);
impl Message for NewDestinations {
    type Result = ();
}

pub struct SaveFromPipe {
    pub destination: String,
    pub dataset: PathBuf,
    pub snapshot: PathBuf,
    pub compression: Option<Compression>,
    pub rx: FileDescriptor,
    pub date: DateTime<Utc>,
}

impl SaveFromPipe {
    pub fn new(
        destination: String,
        dataset: PathBuf,
        snapshot: PathBuf,
        compression: Option<Compression>,
        rx: FileDescriptor,
        date: DateTime<Utc>,
    ) -> Self {
        SaveFromPipe {
            destination,
            dataset,
            snapshot,
            compression,
            rx,
            date,
        }
    }
}

impl Message for SaveFromPipe {
    type Result = Result<(), String>;
}
