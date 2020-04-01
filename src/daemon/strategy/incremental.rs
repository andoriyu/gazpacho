use uclicious::Uclicious;

#[derive(Uclicious, Clone, Debug, Hash)]
#[ucl(skip_builder)]
pub struct Incremental {
    pub zpool: String,
    pub filter: String,
    #[ucl(default)]
    pub runs_before_reset: Option<i64>,
    #[ucl(default)]
    pub days_before_reset: Option<i64>,
}
