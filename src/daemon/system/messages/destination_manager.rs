use crate::daemon::config::Compression;
use crate::daemon::destination::Destination;
use actix::Message;
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
}

impl SaveFromPipe {
    pub fn new(
        destination: String,
        dataset: PathBuf,
        snapshot: PathBuf,
        compression: Option<Compression>,
        rx: FileDescriptor,
    ) -> Self {
        SaveFromPipe {
            destination,
            dataset,
            snapshot,
            compression,
            rx,
        }
    }
}

impl Message for SaveFromPipe {
    type Result = Result<(), String>;
}
