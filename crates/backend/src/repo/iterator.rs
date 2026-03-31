use crate::RepoVfsBackend;
use rustic_core::vfs::Vfs;
use rustic_core::{DataFile, DataFilterOptions, DataIterator, DataLister, Node, RepoIndexed};
use std::{
    io::{self, ErrorKind},
    path::{Path, PathBuf},
    sync::Arc,
};
use std::path::Component;

pub struct RepoVfsLister {
    repo: Arc<RepoIndexed>,
    vfs: Arc<Vfs>,
    path: PathBuf,
    items: Vec<Node>,
    recursive: bool,
}

impl RepoVfsLister {
    fn filter_path(path: &Path) -> PathBuf {
        path
            .components()
            .filter(|c| *c != Component::RootDir)
            .collect()
    }
    pub fn new(
        repo: Arc<RepoIndexed>,
        vfs: Arc<Vfs>,
        path: impl AsRef<Path>,
        recursive: bool, // new flag
    ) -> io::Result<Self> {
        let path = Self::filter_path(path.as_ref());
        let items = vfs
            .dir_entries_from_path(&repo, &path)
            .map_err(|x| io::Error::new(ErrorKind::Other, x))?;

        Ok(Self {
            repo,
            vfs,
            path,
            items,
            recursive,
        })
    }

    /// Internal helper to recursively gather items
    fn gather_items(
        &self,
        be: Arc<RepoVfsBackend>,
        base_path: PathBuf,
        entries: &[Node],
        out: &mut Vec<io::Result<DataFile>>,
    ) -> io::Result<()> {
        for entry in entries {
            let full_path = Self::filter_path(&base_path.join(&entry.name));
            let file = be.raw_get_file(&full_path, entry.clone());
            out.push(Ok(file.clone()));
            if self.recursive && entry.is_dir() {
                // Read directory entries for recursion
                let children = self
                    .vfs
                    .dir_entries_from_path(&self.repo, &full_path)
                    .map_err(|x| io::Error::new(ErrorKind::Other, x))?;
                self.gather_items(be.clone(), full_path, &children, out)?;
            }
        }
        Ok(())
    }
}

impl DataLister for RepoVfsLister {
    fn size(&self) -> io::Result<Option<u64>> {
        Ok(Some(self.items.iter().map(|x| x.meta.size).sum()))
    }

    fn options(&self) -> DataFilterOptions {
        DataFilterOptions::default()
    }

    fn get_iter(self: Arc<Self>) -> io::Result<Box<dyn DataIterator>> {
        let be = Arc::new(RepoVfsBackend::new_with_vfs(
            self.repo.clone(),
            self.vfs.clone(),
        ));

        let mut all_items = Vec::new();
        self.gather_items(be.clone(), self.path.clone(), &self.items, &mut all_items)?;

        // Return an iterator over the Result<DataFile>
        let iter = all_items.into_iter();
        Ok(Box::new(iter))
    }

    fn path(&self) -> &Path {
        &self.path
    }
}
