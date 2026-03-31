use bytes::Bytes;
use rustic_core::vfs::{OpenFile, Vfs};
use rustic_core::{DataLister, DataRead, DataReadWrite, FileOpHandle, Node, RepoIndexed, SeqWrite};
use std::io;
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use crate::repo::iterator::RepoVfsLister;

pub struct RepoVfsHandle {
    pub vfs: Arc<Vfs>,
    pub repo: Arc<RepoIndexed>,
    pub path: PathBuf,
    pub node: Node,
}

impl RepoVfsHandle {
    pub fn new(repo: Arc<RepoIndexed>, vfs: Arc<Vfs>, path: impl AsRef<Path>, node: Node) -> Self {
        Self {
            repo,
            vfs,
            path: path.as_ref().into(),
            node,
        }
    }
}

impl FileOpHandle for RepoVfsHandle {
    fn can_read(&self) -> bool {
        true
    }

    fn can_append(&self) -> bool {
        false
    }

    fn can_full(&self) -> bool {
        false
    }

    fn open_read(&self) -> io::Result<Box<dyn DataRead>> {
        let ret = self
            .repo
            .clone()
            .open_file(&self.node)
            .map_err(|x| io::Error::new(ErrorKind::HostUnreachable, x))?;
        Ok(Box::new(ret))
    }

    fn open_append(&self, _truncate: bool) -> io::Result<Box<dyn SeqWrite>> {
        Err(ErrorKind::Unsupported.into())
    }

    fn open_full(&self) -> io::Result<Box<dyn DataReadWrite>> {
        Err(ErrorKind::Unsupported.into())
    }

    fn read_dir(&self) -> io::Result<Arc<dyn DataLister>> {
        let ret = RepoVfsLister::new(self.repo.clone(), self.vfs.clone(), &self.path, false)?;
        Ok(Arc::new(ret))
    }

    fn delete(&self) -> io::Result<()> {
        Err(ErrorKind::Unsupported.into())
    }

    fn rename(&self, dest: &Path) -> io::Result<()> {
        Err(ErrorKind::Unsupported.into())
    }
}