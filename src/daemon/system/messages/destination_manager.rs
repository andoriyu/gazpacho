use crate::daemon::destination::Destination;
use actix::Message;
use std::collections::HashMap;

pub struct NewDestinations(pub HashMap<String, Destination>);
impl Message for NewDestinations {
    type Result = ();
}
