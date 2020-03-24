use crate::daemon::config::Compression;
use crate::daemon::destination::{Destination, DestinationSsh};
use chrono::Utc;
use libzetta::zfs::PathExt;
use ssh2::{File as SftpFile, Session, Sftp};
use std::fs::File as LocalFile;
use std::io::{Error, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};

pub enum EnsuredDestination {
    SftpFile(SftpFile, Session, Sftp),
    LocalFile(LocalFile),
}

impl EnsuredDestination {
    pub fn ensure(
        dst: &Destination,
        dataset: PathBuf,
        snapshot: PathBuf,
        compression: &Option<Compression>,
    ) -> Self {
        let file_ext = {
            if compression.is_some() {
                "zfs.zstd"
            } else {
                "zfs"
            }
        };

        let today = Utc::today();
        let dst_file_name = {
            let date = today.format("%Y%m%d");
            let basename = dataset.to_string_lossy().replace("/", "_");
            let filename = format!("{}-{}.{}", date, basename, file_ext);
            PathBuf::from(filename)
        };
        let date_folder = {
            let mut path = PathBuf::new();
            let year = today.format("%Y");
            let month = today.format("%m");
            path.push(PathBuf::from(year.to_string()));
            path.push(PathBuf::from(month.to_string()));
            path
        };
        if let Some(dst_ssh) = &dst.ssh {
            return Self::ensure_sftp_file(&dst_ssh, date_folder, dst_file_name);
        }
        unimplemented!();
    }
    fn ensure_sftp_file(dst: &DestinationSsh, date_folder: PathBuf, dst_file: PathBuf) -> Self {
        let dst_folder = {
            let mut path = PathBuf::from(&dst.folder);
            path.push(date_folder);
            path
        };
        let full_dst_file_path = {
            let mut path = PathBuf::from(&dst_folder);
            path.push(dst_file);
            path
        };

        let mut sess = Session::new().unwrap();
        let tcp = TcpStream::connect(&dst.host).unwrap();
        sess.set_tcp_stream(tcp);
        sess.handshake().unwrap();
        sess.userauth_pubkey_file(&dst.username, None, &dst.identity_file, None)
            .unwrap();

        let mut channel = sess.channel_session().unwrap();
        let cmd = format!("mkdir -p {}", dst_folder.to_string_lossy());
        channel.exec(&cmd);

        sess.keepalive_send().unwrap();
        dbg!(&full_dst_file_path);
        let sftp = sess.sftp().unwrap();
        let file = sftp.create(&full_dst_file_path).unwrap();
        EnsuredDestination::SftpFile(file, sess, sftp)
    }
}

impl Write for EnsuredDestination {
    #[inline(always)]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            EnsuredDestination::SftpFile(f, _, _) => f.write(buf),
            EnsuredDestination::LocalFile(f) => f.write(buf),
        }
    }

    #[inline(always)]
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            EnsuredDestination::SftpFile(f, _, _) => f.flush(),
            EnsuredDestination::LocalFile(f) => f.flush(),
        }
    }
}
