use ssh2::Session;
use std::net::{SocketAddr, TcpStream};
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
    #[ucl(default)]
    pub ssh: Option<DestinationSsh>,
    #[ucl(default)]
    pub local: Option<DestinationLocal>,
}

impl Destination {
    pub fn get_ssh_session(&self) -> Option<Session> {
        if let Some(ref dst) = self.ssh {
            return Some(dst.get_ssh_session());
        } else {
            None
        }
    }
}
impl DestinationSsh {
    pub fn get_ssh_session(&self) -> Session {
        let mut sess = Session::new().unwrap();
        let tcp = TcpStream::connect(&self.host).unwrap();
        sess.set_tcp_stream(tcp);
        sess.handshake().unwrap();
        sess.userauth_pubkey_file(&self.username, None, &self.identity_file, None)
            .unwrap();
        sess
    }
}
