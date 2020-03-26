use crate::daemon::logging::GlobalLogger;
use crate::daemon::system::messages::lifecycle::Signals;
use actix::{
    Actor, AsyncContext, Context, Handler, Supervised, System, SystemRegistry, SystemService,
};
use slog::Logger;
use slog::{debug, info, o, warn};

pub struct LifecycleManager {
    logger: Logger,
}

impl Default for LifecycleManager {
    fn default() -> Self {
        let logger =
            GlobalLogger::get().new(o!("module" => module_path!(), "actor" => "LifecycleManager"));
        LifecycleManager { logger }
    }
}

impl Actor for LifecycleManager {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        debug!(&self.logger, "Actor started");
    }

    fn stopped(&mut self, ctx: &mut Self::Context) {
        debug!(&self.logger, "Actor stopped");
    }
}

impl SystemService for LifecycleManager {
    fn service_started(&mut self, _ctx: &mut Context<Self>) {
        debug!(&self.logger, "Actor starting as a system service");
    }
}

impl Supervised for LifecycleManager {
    fn restarting(&mut self, _ctx: &mut Self::Context) {
        warn!(&self.logger, "Actor restarted")
    }
}

impl Handler<Signals> for LifecycleManager {
    type Result = ();

    fn handle(&mut self, msg: Signals, _ctx: &mut Context<Self>) -> Self::Result {
        match msg {
            Signals::SIGINT => {
                warn!(self.logger, "Received SIGINT. Shutting down the system");
                System::current().stop();
            }
        };
    }
}
