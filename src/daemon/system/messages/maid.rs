use actix::Message;

#[derive(Clone)]
pub struct Cleanup(pub Option<String>);

impl Message for Cleanup {
    type Result = ();
}

impl Default for Cleanup {
    fn default() -> Self {
        Self(None)
    }
}
