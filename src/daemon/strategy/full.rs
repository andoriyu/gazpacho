use uclicious::Uclicious;

#[derive(Uclicious, Clone, Debug, Hash)]
#[ucl(skip_builder)]
pub struct FullReplication {
    pub zpool: String,
    pub filter: String,
}
