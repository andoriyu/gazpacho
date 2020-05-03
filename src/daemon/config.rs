use crate::daemon::destination::Destination;
use crate::daemon::strategy::Strategy;
use chrono::Duration;
use std::collections::HashMap;
use std::path::PathBuf;
use uclicious::Uclicious;

#[derive(Uclicious, Clone, Debug)]
#[ucl(skip_builder)]
pub struct ZstdCompression {
    #[ucl(default = "3")]
    pub level: i32,
    #[ucl(default = "1")]
    pub workers: u32,
}

#[derive(Uclicious, Clone, Debug)]
#[ucl(skip_builder)]
pub struct Compression {
    pub zstd: ZstdCompression,
}

#[derive(Uclicious, Clone, Debug)]
#[ucl(skip_builder)]
pub struct Task {
    pub destination: String,
    pub strategy: Strategy,
    #[ucl(default)]
    pub compression: Option<Compression>,
    #[ucl(default = "1")]
    pub parallelism: u32,
}

#[derive(Uclicious, Clone, Debug, Default)]
#[ucl(skip_builder)]
pub struct Log {
    #[ucl(default)]
    pub syslog: Option<LogSysLog>,
    #[ucl(default)]
    pub terminal: LogTerminal,
}

#[derive(Uclicious, Clone, Debug)]
#[ucl(skip_builder)]
pub struct LogTerminal {
    #[ucl(default = "true")]
    pub enabled: bool,
    #[ucl(default = "\"INFO\".to_string()")]
    pub level: String,
}

impl Default for LogTerminal {
    fn default() -> Self {
        LogTerminal {
            enabled: true,
            level: "INFO".to_string(),
        }
    }
}

#[derive(Uclicious, Clone, Debug)]
#[ucl(skip_builder)]
pub struct LogSysLog {
    #[ucl(default = "false")]
    pub enabled: bool,
    #[ucl(default = "\"INFO\".to_string()")]
    pub level: String,
    #[ucl(default = "::std::path::PathBuf::from(\"/var/run/log\")")]
    pub socket: PathBuf,
}

#[derive(Uclicious, Clone, Debug)]
#[ucl(skip_builder)]
pub struct Daemon {
    pub database: PathBuf,
    #[ucl(default, map = "crate::utils::time_to_chrono")]
    pub cleanup_interval: Option<Duration>,
    #[ucl(default = "false")]
    pub cleanup_on_startup: bool,
}

#[derive(Uclicious, Clone, Debug)]
pub struct Configuration {
    pub daemon: Daemon,
    #[ucl(path = "destination")]
    pub destinations: HashMap<String, Destination>,
    #[ucl(path = "task")]
    pub tasks: HashMap<String, Task>,
    #[ucl(default)]
    pub logging: Log,
    #[ucl(default = "1")]
    pub parallelism: u32,
}
