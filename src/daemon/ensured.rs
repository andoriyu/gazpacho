use crate::daemon::config::Compression;
use crate::daemon::destination::{Destination, DestinationLocal, DestinationSsh};
use chrono::{DateTime, Utc};
use slog::{debug, error, trace, Logger};
use ssh2::{File as SftpFile, OpenFlags, OpenType, Session, Sftp};
use std::fmt::{Display, Formatter};
use std::fs::{File as LocalFile, File};
use std::io::Write;
use std::net::TcpStream;
use std::path::{Path, PathBuf};

pub enum EnsuredError {
    Ssh(ssh2::Error),
    Io(std::io::Error),
    MissingConfiguration,
    DuplicateConfiguration,
    RootFolderNotFound(PathBuf),
}

impl Display for EnsuredError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EnsuredError::Ssh(e) => write!(f, "{}", e),
            EnsuredError::MissingConfiguration => write!(f, "Missing destination configuration"),
            EnsuredError::Io(e) => write!(f, "{}", e),
            EnsuredError::DuplicateConfiguration => {
                write!(f, "Duplicate destination configuration")
            }
            EnsuredError::RootFolderNotFound(e) => {
                write!(f, "Destination root folder `{}` doesn't exist", e.display())
            }
        }
    }
}

impl From<ssh2::Error> for EnsuredError {
    fn from(src: ssh2::Error) -> Self {
        EnsuredError::Ssh(src)
    }
}

impl From<std::io::Error> for EnsuredError {
    fn from(src: std::io::Error) -> Self {
        EnsuredError::Io(src)
    }
}

pub enum EnsuredDestination {
    SftpFile(SftpFile, Session, Sftp),
    LocalFile(LocalFile),
}

impl EnsuredDestination {
    pub fn ensure(
        logger: &Logger,
        dst: &Destination,
        dataset: PathBuf,
        compression: &Option<Compression>,
        _logger: &Logger,
        today: DateTime<Utc>,
    ) -> Result<Self, EnsuredError> {
        let file_ext = {
            if compression.is_some() {
                "zfs.zst"
            } else {
                "zfs"
            }
        };

        let dst_file_name = {
            let date = today.format("%Y%m%d");
            let timestamp = today.timestamp();
            let basename = dataset.to_string_lossy().replace("/", "_");
            let filename = format!("{}-{}-{}.{}", date, timestamp, basename, file_ext);
            PathBuf::from(filename)
        };
        let date_folder = {
            let mut path = PathBuf::new();
            let year = today.format("%Y");
            let month = today.format("%m");
            let day = today.format("%d");
            path.push(PathBuf::from(year.to_string()));
            path.push(PathBuf::from(month.to_string()));
            path.push(PathBuf::from(day.to_string()));
            path
        };
        match (&dst.ssh, &dst.local) {
            (None, None) => Err(EnsuredError::MissingConfiguration),
            (Some(dst_ssh), None) => Self::ensure_sftp_file(
                logger,
                dst_ssh,
                date_folder,
                dst_file_name,
                dst.chmod,
                dst.chmod_dir,
            ),
            (None, Some(dst_local)) => {
                Self::ensure_local_file(logger, dst_local, date_folder, dst_file_name)
            }
            (Some(_), Some(_)) => Err(EnsuredError::DuplicateConfiguration),
        }
    }
    fn ensure_local_file(
        _logger: &Logger,
        dst: &DestinationLocal,
        date_folder: PathBuf,
        dst_file: PathBuf,
    ) -> Result<Self, EnsuredError> {
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
        std::fs::create_dir_all(&dst_folder)?;
        let file = File::create(&full_dst_file_path)?;
        Ok(EnsuredDestination::LocalFile(file))
    }

    fn ensure_sftp_file(
        logger: &Logger,
        dst: &DestinationSsh,
        date_folder: PathBuf,
        dst_file: PathBuf,
        chmod: i32,
        chmod_dir: i32,
    ) -> Result<Self, EnsuredError> {
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
        debug!(
            logger,
            "Full path to destination: {}",
            full_dst_file_path.display()
        );

        let mut sess = Session::new()?;
        let tcp = TcpStream::connect(&dst.host)?;
        sess.set_tcp_stream(tcp);
        sess.handshake()?;
        sess.userauth_pubkey_file(&dst.username, None, &dst.identity_file, None)?;
        debug!(logger, "Established ssh session with remote server");
        let sftp = sess.sftp().map_err(|e| {
            error!(logger, "{}", e);
            e
        })?;
        trace!(logger, "Established SFTP channel");
        ensure_root_dir_ssh(&sftp, &dst, chmod_dir)?;
        trace!(logger, "Ensured root folder");
        ensure_dst_dir_ssh(&sftp, &dst, &dst_folder, chmod_dir)?;
        trace!(logger, "Ensured dst folder");
        let open_flags = OpenFlags::WRITE | OpenFlags::TRUNCATE;
        let file = sftp.open_mode(&full_dst_file_path, open_flags, chmod, OpenType::File)?;
        Ok(EnsuredDestination::SftpFile(file, sess, sftp))
    }
}

fn ensure_root_dir_ssh(sftp: &Sftp, dst: &DestinationSsh, chmod: i32) -> Result<(), EnsuredError> {
    let dir = &dst.folder;

    // Unless you're dumping it to `/` this should always be Some.
    if let Some(root) = dir.parent() {
        sftp.stat(root)
            .map_err(|_| EnsuredError::RootFolderNotFound(root.to_path_buf()))?;
    }

    if sftp.stat(dir).is_err() {
        let r = sftp.mkdir(dir, chmod);
        if let Err(e) = r {
            return Err(e.into());
        }
    }
    Ok(())
}

fn ensure_dst_dir_ssh(
    sftp: &Sftp,
    dst: &DestinationSsh,
    dst_folder: &Path,
    chmod: i32,
) -> Result<(), EnsuredError> {
    let err = dst_folder
        .ancestors()
        .filter(|a| a.starts_with(&dst.folder))
        .collect::<Vec<&Path>>()
        .iter()
        .rev()
        .map(|dir| {
            if sftp.stat(dir).is_err() {
                sftp.mkdir(dir, chmod).map(|_| ())
            } else {
                Ok(())
            }
        })
        .find(|res| res.is_err())
        .map(|res| res.unwrap_err());
    if let Some(e) = err {
        Err(e.into())
    } else {
        Ok(())
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
