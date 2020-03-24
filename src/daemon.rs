use uclicious::{Priority, DEFAULT_DUPLICATE_STRATEGY};

pub mod config;
pub mod context;
pub mod destination;
pub mod ensured;
pub mod logging;

use slog::{info};
use logging::GlobalLogger as Log;
use config::Configuration;
use std::time::Duration;

pub fn start_daemon() {
    let input = r#"
        logging syslog {
            enabled = on
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

    logging::setup_root_logger(&conf);
    //let logger = slog::Logger::new(Log::get(), o!("module" => module_path!()));
    info!(Log::get(), "Starting Gazpacho actor system"; "module" => module_path!());

    std::thread::sleep(Duration::from_secs(5));
}
