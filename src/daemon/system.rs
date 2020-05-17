use crate::daemon::logging::GlobalLogger;
use crate::daemon::system::actors::lifecycle::LifecycleManager;
use crate::daemon::system::actors::maid::Maid;
use crate::daemon::system::actors::task_manager::messages::ExecuteTask;
use crate::daemon::system::actors::task_manager::TaskManager;
use crate::daemon::system::messages::lifecycle::Signals;
use actix::prelude::*;
use actix::{System, SystemService};
use slog::{debug, o};
use std::sync::mpsc;
use std::thread::{sleep, JoinHandle};
use std::time::Duration;

pub mod actors;
pub mod futures;
pub mod messages;

pub fn bootstrap_system(tx: mpsc::Sender<(Addr<LifecycleManager>, Addr<Maid>)>) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let log = GlobalLogger::get().new(o!("module" => module_path!()));
        debug!(log, "Starting Gazpacho Actor System");
        let system = System::new("gazpacho");
        let lcma = LifecycleManager::from_registry();
        ctrlc::set_handler(move || {
            lcma.do_send(Signals::SIGINT);
        })
        .expect("Failed to install SIGINT handler");

        let task_registry = TaskManager::from_registry();
        std::thread::spawn(move || {
            sleep(Duration::from_secs(5));
            task_registry.do_send(ExecuteTask(String::from("test")));
        });
        tx.send((LifecycleManager::from_registry(), Maid::from_registry()))
            .unwrap();
        drop(tx);
        system.run().unwrap();
    })
}

fn shutdown() {
    let addr = LifecycleManager::from_registry();
    addr.do_send(Signals::SIGINT);
}
