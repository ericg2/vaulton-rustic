use crate::filters::DataFilter;
use crate::local::backend::LocalBackend;
use crate::local::handle::LocalHandle;
use ignore::{DirEntry, Walk, WalkBuilder};
use itertools::Itertools;
use log::warn;
use rustic_core::{
    DataFile, DataFilterOptions, DataIterator, DataLister, ExtMetadata, RusticError, RusticResult,
};
use std::io;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct LocalLister {
    pub(crate) abs: PathBuf,
    pub(crate) path: PathBuf,
    pub(crate) options: DataFilterOptions,
    pub(crate) walk: WalkBuilder,
}

impl LocalLister {
    pub(crate) fn new(
        abs: &Path,
        path: &Path,
        options: Option<DataFilterOptions>,
        recursive: bool,
    ) -> std::io::Result<Self> {
        let options = options.unwrap_or_default();
        let walk = DataFilter::new(&options)?.to_walker(&[path], recursive)?;
        Ok(Self {
            abs: abs.into(),
            path: path.into(),
            options,
            walk,
        })
    }
}

impl DataLister for LocalLister {
    fn size(&self) -> io::Result<Option<u64>> {
        let mut size = 0;
        for entry in self.walk.build() {
            if let Err(err) = entry.and_then(|e| e.metadata()).map(|m| {
                size += if m.is_dir() { 0 } else { m.len() };
            }) {
                warn!("ignoring error {err}");
            }
        }
        Ok(Some(size))
    }

    fn options(&self) -> DataFilterOptions {
        self.options.clone()
    }

    fn get_iter(self: Arc<Self>) -> io::Result<Box<dyn DataIterator>> {
        let ret = LocalIterator {
            walker: self.walk.build(),
            path: self.abs.clone(),
        };
        Ok(Box::new(ret))
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

pub struct LocalIterator {
    /// The walk iterator.
    walker: Walk,
    path: PathBuf,
}

impl Iterator for LocalIterator {
    type Item = io::Result<DataFile>;

    fn next(&mut self) -> Option<Self::Item> {
        self.walker.next().map(|res| {
            res.map_err(|e| io::Error::new(ErrorKind::Other, e))
                .and_then(|x| map_entry(&self.path, x))
        })
    }

    //
    // fn next(&mut self) -> Option<Self::Item> {
    //     self.walker.next().map(|e| {
    //         map_entry(e.map_err(|err| {
    //             RusticError::with_source(
    //                 ErrorKind::Internal,
    //                 "Failed to get next entry from walk iterator.",
    //                 err,
    //             )
    //                 .ask_report()
    //         })?)
    //             .map_err(|err| {
    //                 RusticError::with_source(
    //                     ErrorKind::Internal,
    //                     "Failed to map Directory entry to ReadSourceEntry.",
    //                     err,
    //                 )
    //                     .ask_report()
    //             })
    //     })
    // }
}

fn map_entry(abs: &Path, entry: DirEntry) -> io::Result<DataFile> {
    let m = entry
        .metadata()
        .map_err(|x| io::Error::new(ErrorKind::Other, format!("Failed to pull metadata: {x}")))?;
    let ext = ExtMetadata {
        path: LocalBackend::get_relative(abs, entry.path()),
        is_dir: m.is_dir(),
        mtime: m.modified().ok().map(|x| x.into()),
        atime: m.accessed().ok().map(|x| x.into()),
        ctime: m.created().ok().map(|x| x.into()),
        uid: None,
        gid: None,
        user: None,
        group: None,
        inode: 0,
        device_id: 0,
        size: m.len(),
        links: 0,
        x_attrs: vec![],
        can_write: true,
    };
    Ok(DataFile::from_xmeta(
        ext,
        LocalHandle::new(abs.to_path_buf(), entry.path().to_path_buf(), m.is_dir()),
    ))
}
