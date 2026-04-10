use crate::RepoIndexed;
use crate::vfs::VfsRepo;
use crate::vfs::fs::Vfs;
use crate::vfs::util::SimpleIgnore;
use arbhx_core::{FilterOptions, MetaStream, Metadata, SizedQuery};
use async_stream::try_stream;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use async_trait::async_trait;
use tokio::runtime::Handle;

pub struct VfsQuery {
    vfs: Arc<Vfs>, 
    repo: Arc<RepoIndexed>,
    root: PathBuf,
    opts: Option<FilterOptions>,
    handle: Handle,
    include_root: bool,
    recursive: bool,
}

impl VfsQuery {
    pub fn new(
        handle: Handle,
        vfs: Arc<Vfs>,
        repo: Arc<RepoIndexed>,
        root: impl Into<PathBuf>,
        opts: Option<FilterOptions>,
        recursive: bool,
        include_root: bool,
    ) -> Self {
        Self {
            vfs,
            repo,
            root: root.into(),
            opts,
            handle,
            recursive,
            include_root,
        }
    }
}

#[async_trait]
impl SizedQuery for VfsQuery {
    async fn size(self: Arc<Self>) -> io::Result<Option<u64>> {
        Ok(None)
    }

    async fn stream(self: Arc<Self>) -> io::Result<Pin<Box<MetaStream>>> {
        let vfs = Arc::clone(&self.vfs);
        let repo = Arc::clone(&self.repo);
        let handle = self.handle.clone();
        let opts = self.opts.clone();
        let root = self.root.clone();

        let ignore = SimpleIgnore::new(&opts.unwrap_or_default())?;
        let stream = try_stream! {
            let mut stack = vec![root.clone()];

            // optional root
            if self.include_root {
                if let Some(meta) = VfsRepo::raw_meta(&self.repo, &self.vfs, &self.root) {
                    if ignore.filter_ok(&meta)? {
                        yield meta;
                    }
                }
            }

            while let Some(dir) = stack.pop() {
                // load directory entries via blocking pool
                let entries = handle.spawn_blocking({
                    let vfs = Arc::clone(&vfs);
                    let repo = Arc::clone(&repo);
                    let dir_clone = dir.clone();
                    move || vfs.dir_entries_from_path(&repo, &dir_clone)
                })
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

                for node in entries {
                    let path = dir.join(node.name());
                    let meta = VfsRepo::raw_meta(&self.repo, &self.vfs, &path).ok_or(io::ErrorKind::Other)?;
                    if !ignore.filter_ok(&meta)? {
                        continue;
                    }
                    else if self.recursive && node.is_dir() {
                        stack.push(path);
                    }

                    yield meta;
                }
            }
        };

        Ok(Box::pin(stream))
    }
}
