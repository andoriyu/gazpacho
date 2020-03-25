use uclicious::{Priority, DEFAULT_DUPLICATE_STRATEGY};

pub mod config;
pub mod context;
pub mod destination;
pub mod ensured;
pub mod logging;
pub mod system;

use slog::{info, debug, error};
use logging::GlobalLogger as Log;
use config::Configuration;
use std::time::Duration;
use crate::daemon::system::bootstrap_system;
use std::sync::mpsc;
use once_cell::sync::OnceCell;

pub static STARTUP_CONFIGURATION: OnceCell<Configuration> = OnceCell::new();

pub fn start_daemon() {
    let input = r#"
        daemon {
            database = "/usr/home/andoriyu/gazpacho.sqlite3"
        }
        logging {
            terminal {
                level = "DEBUG"
            }
        }
        destination "fulcrum" {
            ssh {
                username = "andoriyu",
                identity_file = "/home/andoriyu/.ssh/id_rsa",
                folder = "/mnt/eden/backups/gazpacho/nimble",
                host = "192.168.86.13:22"
            }
        }
        task "test" {
            destination = "fulcrum",
            full_replication {
                zpool = "z",
                filter = "z\/usr\/ports$"
            }
            compression {
                zstd {
                    level = 16,
                    workers = 4,
                }
            }
        }
    "#;
    let mut builder = Configuration::builder().unwrap();
    builder
        .add_chunk_full(input, Priority::default(), DEFAULT_DUPLICATE_STRATEGY)
        .unwrap();
    let conf = builder.build().unwrap();

    dbg!(&conf);

    STARTUP_CONFIGURATION.set(conf.clone()).expect("Failed to set STARTUP_CONFIGURATION");

    logging::setup_root_logger(&conf);
    //let logger = slog::Logger::new(Log::get(), o!("module" => module_path!()));
    info!(Log::get(), "Starting Gazpacho"; "module" => module_path!());
    match slog_stdlog::init() {
        Ok(()) => debug!(Log::get(), "Installed stdlog backend"),
        Err(e) => error!(Log::get(), "Failed to install stdlog backend: {}", e)
    };

    let (tx, rx) = mpsc::channel();

    let system_handle = bootstrap_system(&conf, tx);
    let lifecycle = rx.recv().unwrap();
    system_handle.join().unwrap();
}
