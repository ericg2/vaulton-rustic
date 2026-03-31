pub(crate) mod file_archiver;
pub(crate) mod parent;
pub(crate) mod tree;
pub(crate) mod tree_archiver;

use chrono::Local;
use log::warn;
use pariter::{IteratorExt, scope};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::cancel::JobCancelToken;
use crate::error::{RusticJobError, RusticJobResult};
use crate::{
    Progress,
    archiver::{
        file_archiver::FileArchiver, parent::Parent, tree::TreeIterator,
        tree_archiver::TreeArchiver,
    },
    backend::{DataLister, decrypt::DecryptFullBackend},
    blob::BlobType,
    error::{ErrorKind, RusticError, RusticResult},
    index::{
        ReadGlobalIndex,
        indexer::{Indexer, SharedIndexer},
    },
    repofile::{configfile::ConfigFile, snapshotfile::SnapshotFile},
};

#[derive(thiserror::Error, Debug, displaydoc::Display)]
/// Tree stack empty
pub struct TreeStackEmptyError;

/// The `Archiver` is responsible for archiving files and trees.
/// It will read the file, chunk it, and write the chunks to the backend.
///
/// # Type Parameters
///
/// * `BE` - The backend type.
/// * `I` - The index to read from.
#[allow(missing_debug_implementations)]
#[allow(clippy::struct_field_names)]
pub struct Archiver<'a, BE: DecryptFullBackend, I: ReadGlobalIndex> {
    /// The `FileArchiver` is responsible for archiving files.
    file_archiver: FileArchiver<'a, BE, I>,

    /// The `TreeArchiver` is responsible for archiving trees.
    tree_archiver: TreeArchiver<'a, BE, I>,

    /// The parent snapshot to use.
    parent: Parent,

    /// The `SharedIndexer` is used to index the data.
    indexer: SharedIndexer<BE>,

    /// The backend to write to.
    be: BE,

    /// The backend to write to.
    index: &'a I,

    /// The `SnapshotFile` to write to.
    snap: SnapshotFile,
}

impl<'a, BE: DecryptFullBackend, I: ReadGlobalIndex> Archiver<'a, BE, I> {
    /// Creates a new `Archiver`.
    ///
    /// # Arguments
    ///
    /// * `be` - The backend to write to.
    /// * `index` - The index to read from.
    /// * `config` - The config file.
    /// * `parent` - The parent snapshot to use.
    /// * `snap` - The `SnapshotFile` to write to.
    ///
    /// # Errors
    ///
    /// * If sending the message to the raw packer fails.
    /// * If converting the data length to u64 fails
    pub fn new(
        be: BE,
        index: &'a I,
        config: &ConfigFile,
        parent: Parent,
        mut snap: SnapshotFile,
    ) -> RusticResult<Self> {
        let indexer = Indexer::new(be.clone()).into_shared();
        let mut summary = snap.summary.take().unwrap_or_default();
        summary.backup_start = Local::now();

        let file_archiver = FileArchiver::new(be.clone(), index, indexer.clone(), config)?;
        let tree_archiver = TreeArchiver::new(be.clone(), index, indexer.clone(), config, summary)?;

        Ok(Self {
            file_archiver,
            tree_archiver,
            parent,
            indexer,
            be,
            index,
            snap,
        })
    }

    /// Archives the given source.
    ///
    /// This will archive all files and trees in the given source.
    ///
    /// # Type Parameters
    ///
    /// * `R` - The type of the source.
    ///
    /// # Arguments
    ///
    /// * `index` - The index to read from.
    /// * `src` - The source to archive.
    /// * `backup_path` - The path to the backup.
    /// * `as_path` - The path to archive the backup as.
    /// * `skip_identical_parent` - skip saving of snapshot if tree is identical to parent tree.
    /// * `p` - The progress bar.
    ///
    /// # Errors
    ///
    /// * If sending the message to the raw packer fails.
    /// * If the index file could not be serialized.
    /// * If the time is not in the range of `Local::now()`.
    pub fn archive(
        mut self,
        abs_path: &Path,
        src: Arc<dyn DataLister>,
        backup_path: &Path,
        as_path: Option<&PathBuf>,
        skip_identical_parent: bool,
        no_scan: bool,
        p: &impl Progress,
        token: JobCancelToken,
    ) -> RusticJobResult<SnapshotFile> {
        let src1 = src.clone();
        let src2 = src.clone();
        std::thread::scope(|s| -> RusticJobResult<_> {
            // determine backup size in parallel to running backup
            token.ensure_good(p)?;
            let src_size_handle = s.spawn(move || {
                if !no_scan && !p.is_hidden() {
                    match src1.size() {
                        Ok(Some(size)) => p.set_length(size),
                        Ok(None) => {}
                        Err(err) => warn!("error determining backup size: {}", err),
                    }
                }
            });

            // filter out errors and handle as_path
            token.ensure_good(p)?;
            let iter = src2
                .get_iter()
                .map_err(|x| {
                    RusticError::with_source(
                        ErrorKind::InputOutput,
                        "Failed to read archive iterator",
                        x,
                    )
                })?
                .take_while(|_| !token.is_cancelled())
                .filter_map(|item| {
                    if token.is_cancelled() {
                        None
                    } else {
                        match item {
                            Err(err) => {
                                warn!("ignoring error: {}", err);
                                None
                            }
                            Ok(x) => {
                                let path = x.path();
                                let node = x.node();
                                let open = x.handle();
                                if path == abs_path {
                                    return None; // *** MAKE SURE IT'S NOT ROOT!
                                }
                                let snapshot_path = if let Some(as_path) = as_path {
                                    as_path
                                        .clone()
                                        .join(path.strip_prefix(backup_path).unwrap())
                                } else {
                                    path
                                };

                                Some(if node.is_dir() {
                                    //print!("Found dir {:?} {:?}", &snapshot_path, &node);
                                    (snapshot_path, node, open)
                                } else {
                                    //print!("Found file {:?} {:?}", &snapshot_path, &node);
                                    (
                                        snapshot_path
                                            .parent()
                                            .expect("file path should have a parent!")
                                            .to_path_buf(),
                                        node,
                                        open,
                                    )
                                })
                            }
                        }
                    }
                });
            // handle beginning and ending of trees
            token.ensure_good(p)?;
            let iter = TreeIterator::new(iter);
            scope(|scope| -> RusticJobResult<_> {
                // use parent snapshot
                iter.filter_map(|item| {
                    if token.is_cancelled() {
                        None
                    } else {
                        match self.parent.process(&self.be, self.index, item) {
                            Ok(item) => Some(item),
                            Err(err) => {
                                warn!("ignoring error reading parent snapshot: {err:?}");
                                None
                            }
                        }
                    }
                })
                // archive files in parallel
                .parallel_map_scoped(scope, |item| {
                    if token.is_cancelled() {
                        return Err(RusticJobError::JobCancelled);
                    } else {
                        self.file_archiver
                            .process(item, p)
                            .map_err(|x| RusticJobError::JobError(x))
                    }
                })
                .readahead_scoped(scope)
                .filter_map(|item| match item {
                    Ok(item) => Some(item),
                    Err(err) => {
                        if !matches!(err, RusticJobError::JobCancelled) {
                            warn!("ignoring error: {}", err.display_log());
                        }
                        None
                    }
                })
                .try_for_each(|item| {
                    if token.is_cancelled() {
                        return Err(RusticJobError::JobCancelled);
                    } else {
                        self.tree_archiver
                            .add(item)
                            .map_err(|x| RusticJobError::JobError(x))
                    }
                })
            })
            .expect("Scoped Archiver thread should not panic!")?;

            src_size_handle
                .join()
                .expect("Scoped Size Handler thread should not panic!");

            token.ensure_good(p)?; // *** early return will help finalize.
            Ok(())
        })?;

        token.ensure_good(p)?; // after finalizing, it's too late to cancel... don't corrupt.
        let stats = self.file_archiver.finalize()?;
        let (id, mut summary) = self.tree_archiver.finalize(self.parent.tree_id())?;
        stats.apply(&mut summary, BlobType::Data);
        self.snap.tree = id;

        self.indexer.write().unwrap().finalize()?;

        summary.finalize(self.snap.time).map_err(|err| {
            RusticError::with_source(
                ErrorKind::Internal,
                "Could not finalize summary, please check the logs for more information.",
                err,
            )
        })?;
        self.snap.summary = Some(summary);

        if !skip_identical_parent || Some(self.snap.tree) != self.parent.tree_id() {
            let id = self.be.save_file(&self.snap)?;
            self.snap.id = id.into();
        }

        p.finish();
        Ok(self.snap)
    }
}
