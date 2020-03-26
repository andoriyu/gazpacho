use crate::daemon::config::Configuration;
use actix::Message;

#[derive(Debug)]
pub struct NewConfiguration(pub Configuration);
impl Message for NewConfiguration {
    type Result = ();
}
