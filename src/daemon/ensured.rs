use crate::daemon::config::Compression;
use crate::daemon::destination::{Destination, DestinationSsh};
use chrono::Utc;
use libzetta::zfs::PathExt;
use ssh2::{File as SftpFile, Session};
use std::fs::File as LocalFile;
use std::io::{Error, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};

pub struct EnsuredDestination(pub PathBuf);

impl EnsuredDestination {
    pub fn ensure(
        dst: &Destination,
        dataset: PathBuf,
        snapshot: PathBuf,
        compression: &Option<Compression>,
    ) -> Self {
        let file_ext = { "zfs.zstd" };
        let dst_file_name = {
            let basename = dataset.to_string_lossy().replace("/", "_");
            let filename = format!("{}.{}", basename, file_ext);
            PathBuf::from(filename)
        };
        let date_folder = {
            let today = Utc::today();
            let year = today.format("%Y");
            let month = today.format("%m");
            let mut path = PathBuf::new();
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

        let mut sess = dst.get_ssh_session();

        let mut channel = sess.channel_session().unwrap();
        let cmd = format!("mkdir -p {}", dst_folder.to_string_lossy());
        channel.exec(&cmd);
        dbg!(&full_dst_file_path);
        EnsuredDestination(full_dst_file_path)
    }
}
