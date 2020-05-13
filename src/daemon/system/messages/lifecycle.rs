use crate::daemon::config::Configuration;
use actix::Message;

pub enum Signals {
    SIGINT,
}

impl Message for Signals {
    type Result = ();
}
