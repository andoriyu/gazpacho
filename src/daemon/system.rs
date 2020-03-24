use crate::daemon::config::Configuration;
use slog::Logger;
use slog::{o, debug};
use crate::daemon::logging::GlobalLogger;
use actix::{System, Actor, Context, Supervisor, SystemService};
use crate::daemon::system::actors::lifecycle::LifecycleManager;
use crate::daemon::system::messages::signals::Signals;
use actix::prelude::*;
use std::thread::JoinHandle;
use std::sync::mpsc;

pub mod actors;
pub mod messages;

pub fn bootstrap_system(config: &Configuration, tx: mpsc::Sender<Addr<LifecycleManager>>) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let log= GlobalLogger::get().new(o!("module" => module_path!()));
        debug!(log, "Starting Gazpacho Actor System");
        let system = System::new("gazpacho");
        let lcma = LifecycleManager::from_registry();
        ctrlc::set_handler(move || {
            lcma.do_send(Signals::SIGINT);
        });
        tx.send(LifecycleManager::from_registry());
        system.run().unwrap();
    })
}

