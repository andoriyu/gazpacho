use std::net::SocketAddr;
use std::path::PathBuf;
use uclicious::Uclicious;

#[derive(Uclicious, Clone, Debug, Hash)]
#[ucl(skip_builder)]
pub struct DestinationSsh {
    pub username: String,
    pub identity_file: PathBuf,
    pub folder: PathBuf,
    pub host: SocketAddr,
}

#[derive(Uclicious, Clone, Debug, Hash)]
#[ucl(skip_builder)]
pub struct DestinationLocal {
    pub folder: PathBuf,
}

#[derive(Uclicious, Clone, Debug, Hash)]
#[ucl(skip_builder)]
pub struct Destination {
    #[ucl(default = "1")]
    pub parallelism: u32,
    #[ucl(default = "0o600")]
    pub chmod: i32,
    #[ucl(default = "0o700")]
    pub chmod_dir: i32,
    #[ucl(default)]
    pub ssh: Option<DestinationSsh>,
    #[ucl(default)]
    pub local: Option<DestinationLocal>,
}
