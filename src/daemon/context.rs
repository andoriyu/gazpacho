use crate::daemon::config::Task;
use crate::daemon::destination::Destination;
use crate::daemon::ensured::EnsuredDestination;
use chrono::prelude::*;
use filedescriptor::{AsRawFileDescriptor, Pipe};
use libzetta::zfs::{DelegatingZfsEngine, Error as ZfsError, SendFlags, ZfsEngine};
use ssh2::Session;
use std::collections::HashMap;
use std::convert::Infallible;
use std::io::{BufReader, BufWriter, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use zstd::Encoder;

#[derive(Debug)]
pub struct ExecutionContext {
    destination: Destination,
    destination_folder: PathBuf,
    datasets: Vec<PathBuf>,
    task_name: String,
    task: Task,
}

impl ExecutionContext {
    pub fn new(
        destination: Destination,
        destination_folder: PathBuf,
        datasets: Vec<PathBuf>,
        task_name: String,
        task: Task,
    ) -> Self {
        ExecutionContext {
            destination,
            destination_folder,
            datasets,
            task_name,
            task,
        }
    }

    fn make_snapshots(&mut self) -> Result<Vec<(PathBuf, PathBuf)>, ZfsError> {
        let mut props = HashMap::with_capacity(3);
        props.insert("gazpacho:task".to_string(), self.task_name.clone());
        let z = DelegatingZfsEngine::new()?;
        let snapshot_name = get_snapshot_name();
        let mut error = None;
        let snapshots: Vec<(PathBuf, PathBuf)> = self
            .datasets
            .iter()
            .map(|dataset| {
                let name = format!("{}@{}", dataset.to_str().unwrap(), &snapshot_name);
                (dataset.clone(), PathBuf::from(name))
            })
            .collect();

        if let Some(e) = error {
            return Err(e);
        }
        dbg!(&snapshots);
        let snapshots_to_create: Vec<PathBuf> = snapshots
            .iter()
            .map(|(_, s)| s)
            .cloned()
            .filter(|s| z.exists(s).unwrap() == false)
            .collect();
        dbg!(&snapshots_to_create);
        z.snapshot(&snapshots_to_create, Some(props))?;
        Ok(snapshots)
    }

    pub fn execute(mut self) -> Result<(), ZfsError> {
        let snapshots = self.make_snapshots().unwrap();
        for (dataset, snapshot) in snapshots {
            let mut ensured_dst = EnsuredDestination::ensure(
                &self.destination,
                dataset,
                snapshot.clone(),
                &self.task.compression,
            );
            if let Some(ref compression) = self.task.compression {
                let mut encoder = Encoder::new(ensured_dst, compression.zstd.level).unwrap();

                encoder.multithread(compression.zstd.workers);
                let mut encoder = encoder.auto_finish();

                save_snapshot_to_writer(&mut encoder, snapshot)
            } else {
                save_snapshot_to_writer(&mut ensured_dst, snapshot)
            }
        }
        Ok(())
    }
}

fn get_snapshot_name() -> String {
    let date = Utc::today().format("%Y%m%d");
    format!("gazpacho-{}", date)
}
fn save_snapshot_to_writer<W: Write>(writer: &mut W, snapshot: PathBuf) {
    let mut pipe = Pipe::new().unwrap();
    let mut rx = pipe.read.try_clone().unwrap();
    let handle_zfs = std::thread::spawn(move || {
        let z = DelegatingZfsEngine::new().unwrap();
        let flags = SendFlags::default();
        z.send_full(snapshot, pipe.write, flags);
    });
    std::io::copy(&mut rx, writer).unwrap();
    handle_zfs.join().unwrap();
}
