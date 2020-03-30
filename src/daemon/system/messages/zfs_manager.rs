use actix::Message;
use filedescriptor::Pipe;
use std::path::PathBuf;

pub struct GetDatasetsForTask {
    pub zpool: String,
    pub filter: String,
}

impl GetDatasetsForTask {
    pub fn new(zpool: String, filter: String) -> Self {
        GetDatasetsForTask { zpool, filter }
    }
}

impl Message for GetDatasetsForTask {
    type Result = Vec<PathBuf>;
}

pub struct MakeSnapshots {
    pub datasets: Vec<PathBuf>,
    pub snapshot: String,
}

impl MakeSnapshots {
    pub fn new(datasets: Vec<PathBuf>, snapshot: String) -> Self {
        MakeSnapshots { datasets, snapshot }
    }
}

impl Message for MakeSnapshots {
    type Result = Result<(), String>;
}

pub struct SendSnapshotToPipe(pub PathBuf, pub Pipe);

impl Message for SendSnapshotToPipe {
    type Result = ();
}
