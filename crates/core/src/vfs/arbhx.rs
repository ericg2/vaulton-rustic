use crate::RepoIndexed;
use crate::vfs::fs::{OpenFile, Vfs};
use crate::vfs::query::VfsQuery;
use arbhx_core::{
    DataRead, DataReadSeek, DataUsage, FilterOptions, Metadata, SizedQuery, VfsBackend, VfsFull,
    VfsReader, VfsSeekWriter, VfsWriter,
};
use async_trait::async_trait;
use futures::io::AllowStdIo;
use std::fmt::Debug;
use std::io;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};
use tokio::runtime::Handle;
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt};
use uuid::Uuid;

#[derive(Debug)]
struct Adapter {
    rdr: Compat<AllowStdIo<OpenFile>>,
}

impl AsyncRead for Adapter {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = Pin::new(&mut self);
        this.poll_read(cx, buf)
    }
}

impl AsyncSeek for Adapter {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        let this = Pin::new(&mut self);
        this.start_seek(position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        let this = Pin::new(&mut self);
        this.poll_complete(cx)
    }
}

impl DataRead for Adapter {}

impl DataReadSeek for Adapter {}

#[derive(Debug)]
pub struct VfsRepo {
    pub(crate) id: Uuid,
    pub(crate) handle: Handle,
    pub(crate) repo: Arc<RepoIndexed>,
    pub(crate) vfs: Arc<Vfs>,
}

impl VfsRepo {
    pub(crate) fn raw_meta(
        repo: &RepoIndexed,
        vfs: &Vfs,
        path: impl AsRef<Path>,
    ) -> Option<Metadata> {
        vfs.node_from_path(&repo, path.as_ref()).ok().map(|node| {
            Metadata::default()
                .set_path(path.as_ref())
                .set_is_dir(node.is_dir())
                .set_size(node.meta.size)
                .set_atime(node.meta.atime.map(|x| x.to_utc()))
                .set_mtime(node.meta.mtime.map(|x| x.to_utc()))
        })
    }
}

#[async_trait]
impl VfsBackend for VfsRepo {
    fn id(&self) -> Uuid {
        self.id
    }

    fn realpath(&self, item: &Path) -> PathBuf {
        item.to_path_buf()
    }

    fn reader(self: Arc<Self>) -> Option<Arc<dyn VfsReader>> {
        Some(self.clone())
    }

    fn writer(self: Arc<Self>) -> Option<Arc<dyn VfsWriter>> {
        None
    }

    fn writer_seek(self: Arc<Self>) -> Option<Arc<dyn VfsSeekWriter>> {
        None
    }

    fn full(self: Arc<Self>) -> Option<Arc<dyn VfsFull>> {
        None
    }

    async fn get_usage(&self) -> io::Result<Option<DataUsage>> {
        Ok(None)
    }
}

#[async_trait]
impl VfsReader for VfsRepo {
    async fn open_read_start(&self, item: &Path) -> std::io::Result<Box<dyn DataRead>> {
        let node = self.vfs.node_from_path(&self.repo, item)?;
        let file = self.repo.clone().open_file(&node)?;
        let rdr = AllowStdIo::new(file).compat();
        Ok(Box::new(Adapter { rdr }))
    }

    async fn open_read_seek(&self, item: &Path) -> std::io::Result<Box<dyn DataReadSeek>> {
        let node = self.vfs.node_from_path(&self.repo, item)?;
        let file = self.repo.clone().open_file(&node)?;
        let rdr = AllowStdIo::new(file).compat();
        Ok(Box::new(Adapter { rdr }))
    }

    async fn get_metadata(&self, item: &Path) -> std::io::Result<Option<Metadata>> {
        Ok(Self::raw_meta(&self.repo, &self.vfs, item))
    }

    async fn list(
        &self,
        item: &Path,
        opts: Option<FilterOptions>,
        recursive: bool,
        include_root: bool,
    ) -> std::io::Result<Arc<dyn SizedQuery>> {
        let handle = VfsQuery::new(
            self.handle.clone(),
            self.vfs.clone(),
            self.repo.clone(),
            item,
            opts,
            recursive,
            include_root,
        );
        Ok(Arc::new(handle))
    }
}
