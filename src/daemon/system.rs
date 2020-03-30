use crate::daemon::config::Configuration;
use crate::daemon::logging::GlobalLogger;
use crate::daemon::system::actors::lifecycle::LifecycleManager;
use crate::daemon::system::actors::task_manager::TaskManager;
use crate::daemon::system::actors::zfs_manager::ZfsManager;
use crate::daemon::system::messages::lifecycle::Signals;
use crate::daemon::system::messages::task_manager::{ExecuteTask, NewConfiguration};
use crate::daemon::STARTUP_CONFIGURATION;
use actix::clock::Duration;
use actix::prelude::*;
use actix::{Actor, Context, Supervisor, System, SystemService};
use slog::Logger;
use slog::{debug, o};
use std::sync::mpsc;
use std::thread::{sleep, JoinHandle};

pub mod actors;
pub mod futures;
pub mod messages;

pub fn bootstrap_system(tx: mpsc::Sender<Addr<LifecycleManager>>) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let log = GlobalLogger::get().new(o!("module" => module_path!()));
        debug!(log, "Starting Gazpacho Actor System");
        let system = System::new("gazpacho");
        let lcma = LifecycleManager::from_registry();
        ctrlc::set_handler(move || {
            lcma.do_send(Signals::SIGINT);
        })
        .expect("Failed to install SIGINT handler");
        tx.send(LifecycleManager::from_registry()).unwrap();
        drop(tx);

        let task_registry = TaskManager::from_registry();
        std::thread::spawn(move || {
            sleep(Duration::from_secs(5));
            task_registry.do_send(ExecuteTask(String::from("test")));
        });
        system.run().unwrap();
    })
}

fn shutdown() {
    let addr = LifecycleManager::from_registry();
    addr.do_send(Signals::SIGINT);
}
