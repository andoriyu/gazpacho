/* use crate::daemon::config::Task;
use crate::daemon::system::actors::zfs_manager::ZfsManager;
use crate::daemon::system::messages::task_manager::ExecuteTaskResult;
use crate::daemon::system::messages::zfs_manager::GetDatasetsForTask;
use actix::prelude::Future;
use actix::Addr;
use slog::{debug, Logger};
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
enum State {
    Starting,
    WaitingOnDatasets,
    GotDatasets(Vec<PathBuf>),
}

pub struct TaskFuture {
    task_name: String,
    task: Task,
    zfs_manager: Addr<ZfsManager>,
    state: State,
    logger: Logger,
}
impl Future for TaskFuture {
    type Output = ExecuteTaskResult;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.state {
                State::Starting => {
                    let context = task.full_replication.as_ref().unwrap();
                    let req =
                        GetDatasetsForTask::new(context.zpool.clone(), context.filter.clone());
                    let res_fut = self.zfs_manager.send(req);
                    match res_fut.poll() {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(datasets) => {
                            debug!(
                                self.logger,
                                "Got {} datasets from ZfsManager for task",
                                datasets.len(),
                                &self.task_name
                            );
                            self.state = State::GotDatasets(datasets.unwrap());
                        }
                    }
                }
                _ => unimplemented!(),
            }
        }
    }
}
*/
