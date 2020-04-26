use uclicious::{Priority, DEFAULT_DUPLICATE_STRATEGY};

pub mod config;
pub mod destination;
pub mod ensured;
pub mod logging;
pub mod strategy;
pub mod system;

use crate::daemon::system::bootstrap_system;
use config::Configuration;
use logging::GlobalLogger as Log;
use once_cell::sync::OnceCell;
use slog::{debug, error, info, warn};
use std::sync::mpsc;

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
        destination "temp" {
            parallelism = 2,
            local {
                folder = "/tmp/gazpacho"
            }
        }
        destination "fulcrum" {
            parallelism = 2,
            ssh {
                username = "andoriyu",
                identity_file = "/home/andoriyu/.ssh/id_rsa",
                folder = "/mnt/eden/backups/gazpacho/nimble",
                host = "192.168.86.13:22"
            }
        }
        task "test" {
            parallelism = 2,
            destination = "fulcrum",

            strategy {
                incremental {
                    zpool = "z",
                    filter = "z\/usr\/ports$",
                    duration_before_reset = 7d,
                }
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

    STARTUP_CONFIGURATION
        .set(conf.clone())
        .expect("Failed to set STARTUP_CONFIGURATION");

    logging::setup_root_logger(&conf);
    if libzetta::GlobalLogger::setup(Log::get()).is_err() {
        warn!(Log::get(), "libZetta logger was already set!");
    }
    info!(Log::get(), "Startup sequence initialized"; "module" => module_path!());
    debug!(Log::get(), "Current configuration: {:?}", &conf);
    match slog_stdlog::init() {
        Ok(()) => debug!(Log::get(), "Installed stdlog backend"),
        Err(e) => error!(Log::get(), "Failed to install stdlog backend: {}", e),
    };

    let (tx, rx) = mpsc::channel();

    let system_handle = bootstrap_system(tx);
    let _lifecycle = rx.recv().unwrap();
    system_handle.join().unwrap();
}
