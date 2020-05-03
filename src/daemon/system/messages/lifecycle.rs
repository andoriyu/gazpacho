use crate::daemon::config::Configuration;
use actix::Message;

pub enum Signals {
    SIGINT,
}

impl Message for Signals {
    type Result = ();
}
#[derive(Debug)]
pub struct NewConfiguration(pub Configuration);
impl Message for NewConfiguration {
    type Result = ();
}
