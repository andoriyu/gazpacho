use crate::daemon::context::ExecutionContext;
use crate::daemon::destination::Destination;
use getset::Getters;
use libzetta::zfs::ZfsEngine;
use std::collections::HashMap;
use std::path::PathBuf;
use strum_macros::EnumString;
use uclicious::{FromObject, ObjectError, ObjectRef, Uclicious};

#[derive(Uclicious, Clone, Debug, Hash)]
#[ucl(skip_builder)]
pub struct FullReplication {
    pub zpool: String,
    pub filter: String,
}

#[derive(Uclicious, Clone, Debug, Hash)]
#[ucl(skip_builder)]
pub struct ZstdCompression {
    #[ucl(default = "3")]
    pub level: i32,
    #[ucl(default = "1")]
    pub workers: u32,
}

#[derive(Uclicious, Clone, Debug, Hash)]
#[ucl(skip_builder)]
pub struct Compression {
    pub zstd: ZstdCompression,
}

#[derive(Uclicious, Clone, Debug, Hash)]
#[ucl(skip_builder)]
pub struct Task {
    pub destination: String,
    #[ucl(default)]
    pub full_replication: Option<FullReplication>,
    #[ucl(default)]
    pub compression: Option<Compression>,
}

#[derive(Uclicious, Clone, Debug)]
#[ucl(skip_builder)]
pub struct Log {
    #[ucl(default)]
    pub syslog: Option<LogSysLog>,
    #[ucl(default)]
    pub terminal: Option<LogTerminal>,
}

#[derive(Uclicious, Clone, Debug)]
#[ucl(skip_builder)]
pub struct LogTerminal {
    #[ucl(default = "true")]
    enabled: bool,
    #[ucl(default = "INFO")]
    level: String,
}

#[derive(Uclicious, Clone, Debug)]
#[ucl(skip_builder)]
pub struct LogSysLog {
    #[ucl(default = "false")]
    enabled: bool,
    #[ucl(default = "INFO")]
    level: String,
}

#[derive(Uclicious)]
pub struct Configuration {
    #[ucl(path = "destination")]
    pub destinations: HashMap<String, Destination>,
    #[ucl(path = "task")]
    pub tasks: HashMap<String, Task>,
}

impl Configuration {
    pub fn get_execution_context_for_task(&self, name: &str) -> Option<ExecutionContext> {
        let task = self.tasks.get(name).unwrap();
        let connection = self
            .destinations
            .get(task.destination.as_str())
            .unwrap()
            .clone();
        let z = libzetta::zfs::DelegatingZfsEngine::new().unwrap();
        let reg =
            regex::Regex::new(task.full_replication.as_ref().unwrap().filter.as_str()).unwrap();

        let fs = z.list_filesystems("z").unwrap();
        let vols = z.list_volumes("z").unwrap();
        let all = fs.into_iter().chain(vols);
        let snapshots: Vec<PathBuf> = all
            .filter(|snap| reg.is_match(snap.to_str().unwrap()))
            .collect();
        Some(ExecutionContext::new(
            connection,
            PathBuf::from(name),
            snapshots,
            name.to_string(),
            task.clone(),
        ))
    }
}
