use crate::daemon::strategy::Cleanup;
use uclicious::Uclicious;

#[derive(Uclicious, Clone, Debug, Hash)]
#[ucl(skip_builder)]
pub struct Full {
    pub zpool: String,
    pub filter: String,
    #[ucl(default)]
    pub cleanup: Option<Cleanup>,
}
