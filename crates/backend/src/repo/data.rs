use crate::repo::handle::RepoVfsHandle;
use crate::repo::iterator::RepoVfsLister;
use rustic_core::vfs::{IdenticalSnapshot, Latest, Vfs};
use rustic_core::{DataFile, DataFilterOptions, DataIterator, DataLister, ExtMetadata, Node, RepoIndexed, RusticResult, UsageStat, VfsReader};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug)]
pub struct RepoVfsBackend {
    pub id: Uuid,
    pub repo: Arc<RepoIndexed>,
    pub vfs: Arc<Vfs>,
}

const PATH_TEMPLATE: &'static str = "[{hostname}]/[{label}]/{time}";
const TIME_TEMPLATE: &'static str = "%Y-%m-%d_%H-%M-%S";

impl RepoVfsBackend {
    pub fn raw_get_file(&self, path: &Path, node: Node) -> DataFile {
        let meta = ExtMetadata::from_meta(path, node.is_dir(), node.meta.clone(), false);
        let handle = RepoVfsHandle::new(self.repo.clone(), self.vfs.clone(), path, node);
        DataFile::from_xmeta(meta, handle)
    }
    pub fn new(repo: Arc<RepoIndexed>) -> RusticResult<Self> {
        let snaps = repo.get_all_snapshots()?;
        let vfs = Arc::new(Vfs::from_snapshots(
            snaps,
            PATH_TEMPLATE,
            TIME_TEMPLATE,
            Latest::AsDir,
            IdenticalSnapshot::AsDir,
        )?);
        Ok(Self { id: Uuid::new_v4(), repo, vfs })
    }
    pub fn new_with_vfs(repo: Arc<RepoIndexed>, vfs: Arc<Vfs>) -> Self {
        Self {
            id: Uuid::new_v4(), repo, vfs
        }
    }
}

impl VfsReader for RepoVfsBackend {
    fn get_id(&self) -> Uuid {
        self.id
    }

    fn get_usage(&self) -> Option<io::Result<UsageStat>> {
        None
    }

    fn get_metadata(&self, item: &Path) -> io::Result<Option<ExtMetadata>> {
        match self.vfs.node_from_path(&self.repo, item) {
            Ok(node) => {
                let meta = ExtMetadata::from_meta(item, node.is_dir(), node.meta, false);
                Ok(Some(meta))
            }
            Err(_) => Ok(None),
        }
    }

    fn read_dir(&self, item: &Path, recursive: bool) -> io::Result<Arc<dyn DataLister>> {

        let ret = RepoVfsLister::new(self.repo.clone(), self.vfs.clone(), item, recursive)?;
        Ok(Arc::new(ret))
    }

    fn get_existing(&self, item: &Path) -> io::Result<Option<DataFile>> {
        match self.vfs.node_from_path(&self.repo, item) {
            Ok(node) => Ok(Some(self.raw_get_file(item, node))),
            Err(_) => Ok(None),
        }
    }
}
