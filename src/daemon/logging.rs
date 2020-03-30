use crate::daemon::config::Configuration;
use once_cell::sync::OnceCell;
use slog::{error, o};
use slog::{Drain, Logger};
use slog_syslog::{Facility, Streamer3164};
use std::borrow::Borrow;
use std::ops::Deref;
use std::str::FromStr;

static GLOBAL_LOGGER: OnceCell<GlobalLogger> = OnceCell::new();

#[derive(Debug, Clone)]
pub struct GlobalLogger {
    inner: Logger,
}
impl Deref for GlobalLogger {
    type Target = Logger;

    fn deref(&self) -> &Self::Target {
        self.inner.borrow()
    }
}

impl GlobalLogger {
    pub fn get() -> &'static GlobalLogger {
        GLOBAL_LOGGER
            .get()
            .expect("Trying to get GLOBAL_LOGGER before it was setup")
    }
}

fn setup_syslog_drain(config: &Configuration) -> Option<slog::Fuse<Streamer3164>> {
    config
        .logging
        .syslog
        .as_ref()
        .map(|conf| {
            if !conf.enabled {
                return None;
            }
            let level = {
                slog::Level::from_str(config.logging.terminal.level.as_str())
                    .unwrap_or_else(|_| slog::Level::Info)
            };
            slog_syslog::SyslogBuilder::new()
                .facility(Facility::LOG_USER)
                .level(level)
                .unix(conf.socket.as_path())
                .start()
                .map(Drain::fuse)
                .ok()
        })
        .flatten()
}

fn setup_terminal_drain(config: &Configuration) -> Option<slog::Fuse<slog_async::Async>> {
    if !config.logging.terminal.enabled {
        return None;
    }
    let level = {
        slog::Level::from_str(config.logging.terminal.level.as_str())
            .unwrap_or_else(|_| slog::Level::Info)
    };
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build();
    let drain = slog::LevelFilter::new(drain, level).fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    Some(drain)
}

fn setup_discard_drain() -> slog::Fuse<slog::Discard> {
    let drain = slog::Discard {};
    drain.fuse()
}

pub fn setup_root_logger(config: &Configuration) {
    let keys = o!("service" => crate::SERVICE, "gazpacho_version" => crate::VERSION);

    let root_logger = match (setup_terminal_drain(config), setup_syslog_drain(config)) {
        (None, None) => Logger::root(setup_discard_drain(), keys),
        (Some(term_drain), None) => Logger::root(term_drain, keys),
        (None, Some(syslog_drain)) => Logger::root(syslog_drain, keys),
        (Some(term_drain), Some(syslog_drain)) => Logger::root(
            slog::Duplicate::new(term_drain, syslog_drain).ignore_res(),
            keys,
        ),
    };
    match GLOBAL_LOGGER.set(GlobalLogger {
        inner: root_logger.clone(),
    }) {
        Ok(()) => {}
        Err(_) => {
            error!(root_logger, "Failed to set GLOBAL_LOGGER!");
        }
    };
}
