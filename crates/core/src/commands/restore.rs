//! `restore` subcommand

use derive_setters::Setters;
use log::{debug, error, info, trace, warn};

use crate::backend::DataFile;
use crate::blob::Blob;
use crate::blob::tree::TreeStreamerOptions;
use crate::cancel::JobCancelToken;
use crate::error::RusticJobResult;
use crate::repository::NodeIterator;
use crate::{
    DataBackends, Id, RepoFilterOptions,
    backend::{
        FileType, ReadBackend,
        decrypt::DecryptReadBackend,
        node::{Node, NodeType},
    },
    error::{ErrorKind, RusticError, RusticResult},
    progress::{Progress, ProgressBars},
    repofile::packfile::PackId,
    repository::{IndexedFull, IndexedTree, Open, Repository},
};
use crate::{LsOptions, join_force};
use arbhx_core::Metadata;
use arbhx_core::blocking::DataWriteSeekCompat;
use bytes::Bytes;
use chrono::{DateTime, Local, Utc};
use ignore::{DirEntry, WalkBuilder};
use itertools::Itertools;
use rayon::{Scope, ThreadPoolBuilder};
use serde_derive::{Deserialize, Serialize};
use serde_with::serde_as;
use std::fmt::Debug;
use std::fs::File;
use std::io::{Read, SeekFrom, Write};
use std::path::Component;
use std::sync::Arc;
use std::{
    cmp::Ordering,
    collections::BTreeMap,
    io,
    num::NonZeroU32,
    path::{Path, PathBuf},
    sync::Mutex,
};

pub(crate) mod constants {
    /// The maximum number of reader threads to use for restoring.
    pub(crate) const MAX_READER_THREADS_NUM: usize = 20;
}

type RestoreInfo = BTreeMap<(PackId, BlobLocation), Vec<FileLocation>>;
type Filenames = Vec<PathBuf>;

/// Determines which network cost the restore process should favor when deciding
/// how to handle existing files at the destination.
///
/// This influences whether the restore planner prefers verifying existing data
/// (which requires downloading parts of files) or rewriting files entirely
/// (which requires uploading more data).
///
/// This is primarily relevant for object storage backends where downloads and
/// uploads may have very different performance or cost characteristics.
///
/// Variants:
///
/// - `PreferDownloads`
///   The restore process will attempt to verify existing files by reading
///   portions of them (for example via range reads) and comparing them with the
///   repository data. Only chunks that differ will be restored.
///
///   This results in **more downloads but fewer uploads**.
///
/// - `PreferUploads`
///   The restore process will avoid verifying existing data and instead
///   rewrite files that may have changed. This skips downloading file data
///   for verification and restores the required chunks directly.
///
///   This results in **fewer downloads but potentially more uploads**.
///
/// The optimal choice depends on the storage backend and network conditions.
/// For example, object storage services often make downloads expensive, making
/// `PreferUploads` a reasonable default.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default, Hash)]
pub enum RestoreBias {
    #[default]
    /// Prefer downloading first. The default [`RestoreBias`] for all storage types.
    PreferDownloads,

    /// Prefer uploading first.
    PreferUploads,
}

impl RestoreBias {
    pub fn download() -> Self {
        Self::PreferDownloads
    }
    pub fn upload() -> Self {
        Self::PreferUploads
    }
}

#[allow(clippy::struct_excessive_bools)]
#[cfg_attr(feature = "clap", derive(clap::Parser))]
#[derive(Debug, Clone, Default, Setters, Serialize, Deserialize)]
#[setters(into)]
#[non_exhaustive]
/// Options for the `restore` command
pub struct RestoreOptions {
    /// Remove all files/dirs in destination which are not contained in snapshot.
    ///
    /// # Warning
    ///
    /// * Use with care, maybe first try this with `--dry-run`?
    #[cfg_attr(feature = "clap", clap(long))]
    pub delete: bool,

    /// Use numeric ids instead of user/group when restoring uid/gui
    #[cfg_attr(feature = "clap", clap(long))]
    pub numeric_id: bool,

    /// Don't restore ownership (user/group)
    #[cfg_attr(feature = "clap", clap(long, conflicts_with = "numeric_id"))]
    pub no_ownership: bool,

    /// Always read and verify existing files (don't trust correct modification time and file size)
    #[cfg_attr(feature = "clap", clap(long))]
    pub verify_existing: bool,

    /// If the restore is a DRY RUN, as in, no files get touched...
    #[cfg_attr(feature = "clap", clap(long))]
    pub dry_run: bool,
}

#[derive(Default, Debug, Clone, Copy)]
#[non_exhaustive]
/// Statistics for files or directories
pub struct FileDirStats {
    /// Number of files or directories to restore
    pub restore: u64,
    /// Number of files or directories which are unchanged (determined by date, but not verified)
    pub unchanged: u64,
    /// Number of files or directories which are verified and unchanged
    pub verified: u64,
    /// Number of files or directories which are modified
    pub modify: u64,
    /// Number of additional entries
    pub additional: u64,
}

#[derive(Default, Debug, Clone, Copy)]
#[non_exhaustive]
/// Restore statistics
pub struct RestoreStats {
    /// file statistics
    pub files: FileDirStats,
    /// directory statistics
    pub dirs: FileDirStats,
}

/// Restore the repository to the given destination.
///
/// # Type Parameters
///
/// * `P` - The progress bar type
/// * `S` - The type of the indexed tree
///
/// # Arguments
///
/// * `file_infos` - The restore information
/// * `repo` - The repository to restore
/// * `opts` - The restore options
/// * `node_streamer` - The node streamer to use
/// * `dest` - The destination to restore to
///
/// # Errors
///
/// * If the restore failed.
pub(crate) fn restore_repository<P, S>(
    file_infos: RestorePlan,
    repo: &Repository<P, S>,
    opts: &RestoreOptions,
    token: JobCancelToken,
    node_streamer: impl Iterator<Item = RusticResult<(PathBuf, Node)>>,
) -> RusticJobResult<()>
where
    P: ProgressBars,
    S: IndexedTree,
{
    let dest = file_infos.be.clone();
    repo.warm_up_wait(file_infos.to_packs().into_iter())?;
    token.ensure_check()?; // *** final check before backing up...
    restore_contents(repo, &dest, token, file_infos)?;

    let p = repo.pb.progress_spinner("setting metadata...");
    //restore_metadata(node_streamer, opts, &dest)?; REMOVED 3-12-26
    p.finish();

    Ok(())
}

/// Collect restore information, scan existing files, create needed dirs and remove superfluous files
///
/// # Type Parameters
///
/// * `P` - The progress bar type.
/// * `S` - The type of the indexed tree.
///
/// # Arguments
///
/// * `repo` - The repository to restore.
/// * `node_streamer` - The node streamer to use.
/// * `dest` - The destination to restore to.
/// * `dry_run` - If true, don't actually restore anything, but only print out what would be done.
///
/// # Errors
///
/// * If a directory could not be created.
/// * If the restore information could not be collected.
#[allow(clippy::too_many_lines)]
pub(crate) fn collect_and_prepare<P, S>(
    repo: &Repository<P, S>,
    opts: &RestoreOptions,
    mut node_streamer: impl NodeIterator,
    dest_be: &DataBackends,
    dest_path: &Path,
    dry_run: bool,
    token: JobCancelToken,
) -> RusticJobResult<RestorePlan>
where
    P: ProgressBars,
    S: IndexedFull,
{
    let p = repo.pb.progress_spinner("collecting file information...");
    token.ensure_good(&p)?;

    let dest = dest_be.repository();
    let mut stats = RestoreStats::default();
    let mut restore_infos = RestorePlan::new(dest_be.clone(), dest_path);
    let mut additional_existing = false;
    let mut removed_dir = None;
    let mut process_existing = |entry: &Metadata| -> RusticResult<_> {
        if entry.path() == dest.realpath(dest_path) {
            // don't process the root dir which should be existing
            return Ok(());
        }
        debug!("additional {}", entry.path().display());
        if entry.is_dir() {
            stats.dirs.additional += 1;
        } else {
            stats.files.additional += 1;
        }
        match (opts.delete, dry_run, entry.is_dir()) {
            (true, true, true) => {
                info!(
                    "would have removed the additional dir: {}",
                    entry.path().display()
                );
            }
            (true, true, false) => {
                info!(
                    "would have removed the additional file: {}",
                    entry.path().display()
                );
            }
            (true, false, true) => {
                let path = entry.path();
                match &removed_dir {
                    Some(dir) if path.starts_with(dir) => {}
                    _ => match dest
                        .clone()
                        .writer()
                        .ok_or(RusticError::new(
                            ErrorKind::Backend,
                            "Delete mode enabled, but backend is not writable",
                        ))?
                        .remove_dir(&path)
                    {
                        Ok(()) => {
                            removed_dir = Some(path.to_path_buf());
                        }
                        Err(err) => {
                            error!("error removing {}: {err}", path.display());
                        }
                    },
                }
            }
            (true, false, false) => {
                if let Err(err) = dest
                    .clone()
                    .writer()
                    .ok_or(RusticError::new(
                        ErrorKind::Backend,
                        "Delete mode enabled, but backend is not writable",
                    ))?
                    .remove_file(&entry.path())
                {
                    error!("error removing {}: {err}", entry.path().display());
                }
            }
            (false, _, _) => {
                additional_existing = true;
            }
        }

        Ok(())
    };

    let mut process_node = |path: &PathBuf, node: &Node, exists: bool| -> RusticResult<_> {
        match node.node_type {
            NodeType::Dir => {
                if exists {
                    stats.dirs.modify += 1;
                    trace!("existing dir {}", path.display());
                } else {
                    stats.dirs.restore += 1;
                    debug!("to restore: {}", path.display());
                    if !dry_run {
                        dest
                            .clone()
                            .writer()
                            .ok_or(RusticError::new(
                                ErrorKind::Backend,
                                "Restore backend is not writable",
                            ))?
                            .create_dir(&join_force(dest_path, path))
                            .map_err(|err| {
                                RusticError::with_source(
                                    ErrorKind::InputOutput,
                                    "Failed to create the directory `{path}`. Please check the path and try again.",
                                    err,
                                )
                                    .attach_context("path", path.display().to_string())
                            })?;
                    }
                }
            }
            NodeType::File => {
                // collect blobs needed for restoring
                match (
                    exists,
                    restore_infos.add_file(
                        dest_be,
                        dest_path,
                        node,
                        path.clone(),
                        repo,
                        opts.verify_existing,
                    )?,
                ) {
                    // Note that exists = false and Existing or Verified can happen if the file is changed between scanning the dir
                    // and calling add_file. So we don't care about exists but trust add_file here.
                    (_, AddFileResult::Existing) => {
                        stats.files.unchanged += 1;
                        trace!("identical file: {}", path.display());
                    }
                    (_, AddFileResult::Verified) => {
                        stats.files.verified += 1;
                        trace!("verified identical file: {}", path.display());
                    }
                    // TODO: The differentiation between files to modify and files to create could be done only by add_file
                    // Currently, add_file never returns Modify, but always New, so we differentiate based on exists
                    (true, AddFileResult::Modify) => {
                        stats.files.modify += 1;
                        debug!("to modify: {}", path.display());
                    }
                    (false, AddFileResult::Modify) => {
                        stats.files.restore += 1;
                        debug!("to restore: {}", path.display());
                    }
                }
            }
            _ => {} // nothing to do for symlink, device, etc.
        }
        Ok(())
    };

    token.ensure_good(&p)?;
    // First, make sure all folders exist for our restore point!
    dest.clone()
        .writer()
        .ok_or(RusticError::new(
            ErrorKind::Backend,
            "Restore backend is not writable",
        ))?
        .create_dir(dest_path)
        .map_err(|err| {
            RusticError::with_source(
                ErrorKind::InputOutput,
                "Failed to create the `{dest_path}`.",
                err,
            )
        })?;

    let mut dst_iter = dest
        .clone()
        .reader()
        .ok_or(RusticError::new(
            ErrorKind::Backend,
            "Restore backend is not readable!",
        ))?
        .list(dest_path, None, true, true) // NEEDS TO BE ABSOLUTE
        .and_then(|x| x.stream())
        .map_err(|err| {
            RusticError::with_source(
                ErrorKind::InputOutput,
                "Failed to create read the `{dest_path}`. Please check the path and try again.",
                err,
            )
        })?
        .filter_map(Result::ok);

    let mut next_dst = dst_iter.next();
    let mut next_node = node_streamer.next().transpose()?;
    loop {
        token.ensure_good(&p)?; // *** check for each directory... might be too much 1-12-26.
        match (&next_dst, &next_node) {
            (None, None) => break,

            (Some(destination), None) => {
                process_existing(destination)?;
                next_dst = dst_iter.next();
            }
            (Some(destination), Some((path, node))) => {
                let abs_path = &dest.realpath(dest_path);
                let cmp_path = join_force(abs_path, path);
                trace!("Comparing {:?} to {:?}", &destination.path(), &cmp_path);
                match destination.path().cmp(&cmp_path) {
                    Ordering::Less => {
                        trace!("Item is less. Considered existing...");
                        process_existing(destination)?;
                        next_dst = dst_iter.next();
                    }
                    Ordering::Equal => {
                        // process existing node
                        if (node.is_dir() && !destination.is_dir())
                            || (node.is_file() && destination.is_dir())
                            || node.is_special()
                        {
                            // if types do not match, first remove the existing file
                            trace!("Item is equal and exists. Special item...");
                            process_existing(destination)?;
                        }
                        trace!("Item is equal and exists. Non-special item...");
                        process_node(path, node, true)?;
                        next_dst = dst_iter.next();
                        next_node = node_streamer.next().transpose()?;
                    }
                    Ordering::Greater => {
                        trace!("Item is not existing.");
                        process_node(path, node, false)?;
                        next_node = node_streamer.next().transpose()?;
                    }
                }
            }
            (None, Some((path, node))) => {
                trace!("Dest item is empty.");
                process_node(path, node, false)?;
                next_node = node_streamer.next().transpose()?;
            }
        }
    }

    if additional_existing {
        warn!("Note: additional entries exist in destination");
    }

    restore_infos.stats = stats;
    p.finish();

    Ok(restore_infos)
}

/// Restore the metadata of the files and directories.
///
/// # Arguments
///
/// * `node_streamer` - The node streamer to use
/// * `opts` - The restore options to use
/// * `dest` - The destination to restore to
///
/// # Errors
///
/// * If the restore failed.
fn restore_metadata(
    mut node_streamer: impl Iterator<Item = RusticResult<(PathBuf, Node)>>,
    opts: &RestoreOptions,
    dest: &DataBackends,
) -> RusticResult<()> {
    let mut dir_stack = Vec::new();
    while let Some((path, node)) = node_streamer.next().transpose()? {
        match node.node_type {
            NodeType::Dir => {
                // set metadata for all non-parent paths in stack
                while let Some((stackpath, _)) = dir_stack.last() {
                    if path.starts_with(stackpath) {
                        break;
                    }
                    let (path, node) = dir_stack.pop().unwrap();
                    set_metadata(dest, opts, &path, &node);
                }
                // push current path to the stack
                dir_stack.push((path, node));
            }
            _ => set_metadata(dest, opts, &path, &node),
        }
    }

    // empty dir stack and set metadata
    for (path, node) in dir_stack.into_iter().rev() {
        set_metadata(dest, opts, &path, &node);
    }

    Ok(())
}

/// Set the metadata of the given file or directory.
///
/// # Arguments
///
/// * `dest` - The destination to restore to
/// * `opts` - The restore options to use
/// * `path` - The path of the file or directory
/// * `node` - The node information of the file or directory
///
/// # Errors
///
/// If the metadata could not be set.
// TODO: Return a result here, introduce errors and get rid of logging.
pub(crate) fn set_metadata(
    dest: &DataBackends,
    opts: &RestoreOptions,
    path: &PathBuf,
    node: &Node,
) {
    debug!("setting metadata for {}", path.display());
    // DISABLED 3-1-26 FOR CLOUD STORAGE BACKEND
    // dest.create_special(path, node)
    //     .unwrap_or_else(|_| warn!("restore {}: creating special file failed.", path.display()));
    // match (opts.no_ownership, opts.numeric_id) {
    //     (true, _) => {}
    //     (false, true) => dest
    //         .set_uid_gid(path, &node.meta)
    //         .unwrap_or_else(|_| warn!("restore {}: setting UID/GID failed.", path.display())),
    //     (false, false) => dest
    //         .set_user_group(path, &node.meta)
    //         .unwrap_or_else(|_| warn!("restore {}: setting User/Group failed.", path.display())),
    // }
    // dest.set_permission(path, node)
    //     .unwrap_or_else(|_| warn!("restore {}: chmod failed.", path.display()));
    // dest.set_extended_attributes(path, &node.meta.extended_attributes)
    //     .unwrap_or_else(|_| {
    //         warn!(
    //             "restore {}: setting extended attributes failed.",
    //             path.display()
    //         );
    //     });
    // dest.set_times(path, &node.meta)
    //     .unwrap_or_else(|_| warn!("restore {}: setting file times failed.", path.display()));
}

/// [`restore_contents`] restores all files contents as described by `file_infos`
/// using the [`DecryptReadBackend`] `be` and writing them into the [`LocalDestination`] `dest`.
///
/// # Type Parameters
///
/// * `P` - The progress bar type.
/// * `S` - The state the repository is in.
///
/// # Arguments
///
/// * `repo` - The repository to restore.
/// * `dest` - The destination to restore to.
/// * `file_infos` - The restore information.
///
/// # Errors
///
/// * If the length of a file could not be set.
/// * If the restore failed.
#[allow(clippy::too_many_lines)]
fn restore_contents<P, S>(
    repo: &Repository<P, S>,
    dest: &DataBackends,
    token: JobCancelToken,
    file_infos: RestorePlan,
) -> RusticJobResult<()>
where
    P: ProgressBars,
    S: Open,
{
    use rayon::prelude::*;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    let dest_path = &file_infos.dest_path;
    let RestorePlan {
        names: filenames,
        file_lengths,
        r: restore_info,
        restore_size: total_size,
        ..
    } = file_infos;

    let filenames = Arc::new(filenames);
    let be = repo.dbe();
    let threads = constants::MAX_READER_THREADS_NUM;
    let pool = ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .map_err(|err| {
            RusticError::with_source(
                ErrorKind::Internal,
                "Failed to create the thread pool with `{num_threads}` threads. Please try again.",
                err,
            )
            .attach_context("num_threads", threads.to_string())
        })?;

    // create empty files first
    let w = dest.repository().writer().ok_or(RusticError::new(
        ErrorKind::Backend,
        "Backend does not support writing for restore!",
    ))?;
    for (i, size) in file_lengths.iter().enumerate() {
        if *size == 0 {
            let path = join_force(dest_path, &filenames[i]);
            w.set_length(&path, 0).map_err(|err| {
                RusticError::with_source(
                    ErrorKind::InputOutput,
                    "Failed to set the length of the file `{path}`.",
                    err,
                )
                .attach_context("path", path.display().to_string())
            })?;
            token.ensure_check()?;
        }
    }

    let p = repo.pb.progress_bytes("restoring file contents...");
    p.set_length(total_size);
    token.ensure_good(&p)?;

    type BlobEntry = (
        u64,
        PackId,
        u32,
        u32,
        Option<NonZeroU32>,
        Option<(usize, u64, u32)>,
    );

    let mut per_file: BTreeMap<usize, Vec<BlobEntry>> = BTreeMap::new();
    for ((pack, bl), fls) in restore_info {
        let from_file = fls
            .iter()
            .find(|fl| fl.matches)
            .map(|fl| (fl.file_idx, fl.file_start, bl.data_length()));

        for fl in fls.iter().filter(|fl| !fl.matches) {
            per_file.entry(fl.file_idx).or_default().push((
                fl.file_start,
                pack.clone(),
                bl.offset,
                bl.length,
                bl.uncompressed_length,
                from_file,
            ));
        }
    }

    for entries in per_file.values_mut() {
        entries.sort_unstable_by_key(|e| e.0);
    }

    let read_blob = |from_file: Option<(usize, u64, u32)>,
                     pack: &PackId,
                     blob_offset: u32,
                     blob_length: u32,
                     uncompressed_length: Option<NonZeroU32>|
     -> Bytes {
        match from_file {
            Some((src_idx, src_offset, src_len)) => {
                // TODO: add better error handling!
                let src_path = join_force(dest_path, &filenames[src_idx]);
                let mut handle = dest
                    .get_existing(&src_path)
                    .unwrap()
                    .open_read_full()
                    .unwrap();
                handle.seek(SeekFrom::Start(src_offset)).unwrap();
                Bytes::from(crate::read_at(&mut handle, src_offset, src_len as usize).unwrap())
            }
            None => {
                let raw = be
                    .read_partial(FileType::Pack, pack, false, blob_offset, blob_length)
                    .unwrap();
                be.read_encrypted_from_partial(&raw, uncompressed_length)
                    .unwrap()
            }
        }
    };

    pool.install(|| {
        per_file.into_par_iter().for_each(|(file_idx, entries)| {
            if token.is_cancelled() {
                return;
            }
            let path = join_force(dest_path, &filenames[file_idx]);
            let d_file = dest.ensure_file(&path).unwrap();
            if dest.supports_random() {
                // Random-write mode: write each blob directly at its intended offset
                let mut handle = d_file.open_write_full().unwrap();
                for (file_start, pack, blob_offset, blob_length, uncompressed_length, from_file) in
                    entries
                {
                    if token.is_cancelled() {
                        return;
                    }
                    let data: Bytes = read_blob(
                        from_file,
                        &pack,
                        blob_offset,
                        blob_length,
                        uncompressed_length,
                    );
                    let size = data.len() as u64;
                    trace!(
                        "Random-writing {} bytes to {:?} at offset {}",
                        size, path, file_start
                    );

                    handle.seek(SeekFrom::Start(file_start)).unwrap();
                    handle.write(&data).unwrap();
                    p.inc(size);
                }
                handle.flush().unwrap();
                handle.close().unwrap();
            } else {
                let mut expected_offset: u64 = 0;
                let mut writer = d_file.open_write(true).unwrap();
                for (file_start, pack, blob_offset, blob_length, uncompressed_length, from_file) in
                    entries
                {
                    if token.is_cancelled() {
                        return;
                    }
                    if file_start != expected_offset {
                        panic!(
                            "Non-sequential restore detected for {:?}: expected {}, got {}",
                            path, expected_offset, file_start
                        );
                    }
                    let data: Bytes = read_blob(
                        from_file,
                        &pack,
                        blob_offset,
                        blob_length,
                        uncompressed_length,
                    );
                    let size = data.len() as u64;
                    trace!(
                        "Writing {} bytes to {:?} (expected_offset={})",
                        size, path, expected_offset
                    );

                    writer.write_all(&data).unwrap();
                    expected_offset += size;
                    p.inc(size);
                }
                writer.flush().unwrap();
                writer.close().unwrap();
            }
        });
    });

    token.ensure_good(&p)?;
    p.finish();
    Ok(())
}

/// Information about what will be restored.
///
/// Struct that contains information of file contents grouped by
/// 1) pack ID,
/// 2) blob within this pack
/// 3) the actual files and position of this blob within those
/// 4) Statistical information
//#[derive(Debug)]
pub struct RestorePlan {
    /// The names of the files to restore
    names: Filenames,
    /// The length of the files to restore
    file_lengths: Vec<u64>,
    /// The restore information
    r: RestoreInfo,
    /// The path to restore to
    dest_path: PathBuf,
    /// The total restore size
    pub restore_size: u64,
    /// The total size of matched content, i.e. content with needs no restore.
    pub matched_size: u64,
    /// Statistics about the restore.
    pub stats: RestoreStats,
    /// The backend to restore to...
    pub be: DataBackends,
}

impl RestorePlan {
    pub fn new(be: DataBackends, dest_path: impl AsRef<Path>) -> Self {
        Self {
            be,
            dest_path: dest_path.as_ref().to_path_buf(),
            names: Default::default(),
            file_lengths: Default::default(),
            r: Default::default(),
            restore_size: Default::default(),
            matched_size: Default::default(),
            stats: Default::default(),
        }
    }
}

/// `BlobLocation` contains information about a blob within a pack
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct BlobLocation {
    /// The offset of the blob within the pack
    offset: u32,
    /// The length of the blob
    length: u32,
    /// The uncompressed length of the blob
    uncompressed_length: Option<NonZeroU32>,
}

impl BlobLocation {
    /// Get the length of the data contained in this blob
    fn data_length(&self) -> u32 {
        self.uncompressed_length.map_or(
            self.length - 32, // crypto overhead
            NonZeroU32::get,
        )
    }
}

/// [`FileLocation`] contains information about a file within a blob
#[derive(Debug, Clone)]
struct FileLocation {
    // TODO: The index of the file within ... ?
    file_idx: usize,
    /// The start of the file within the blob
    file_start: u64,
    /// Whether the file matches the blob
    ///
    /// This indicates that the file exists and these contents are already correct.
    matches: bool,
}

/// [`AddFileResult`] indicates the result of adding a file to [`FileLocation`]
// TODO: Add documentation!
enum AddFileResult {
    Existing,
    Verified,
    Modify,
}

impl RestorePlan {
    /// Add the file to [`FileLocation`] using `index` to get blob information.
    ///
    /// # Type Parameters
    ///
    /// * `P` - The progress bar type.
    /// * `S` - The type of the indexed tree.
    ///
    /// # Arguments
    ///
    /// * `dest` - The destination to restore to.
    /// * `file` - The file to add.
    /// * `name` - The name of the file.
    /// * `repo` - The repository to restore.
    /// * `ignore_mtime` - If true, ignore the modification time of the file.
    ///
    /// # Errors
    ///
    /// * If the file could not be added.
    fn add_file<P, S: IndexedFull>(
        &mut self,
        dest_be: &DataBackends,
        abs_path: &Path,
        file: &Node,
        name: PathBuf,
        repo: &Repository<P, S>,
        ignore_mtime: bool,
    ) -> RusticResult<AddFileResult> {
        let dest = dest_be.repository();
        let path = join_force(abs_path, &name);
        let open_file = dest_be
            .get_matching_file(&path, file.meta.size)
            .map_err(|err| {
                RusticError::with_source(ErrorKind::InputOutput, "Failed to get matching file", err)
            })?;

        // Check empty files first
        if file.meta.size == 0 {
            if let Some(meta) = open_file.as_ref().map(|x| x.meta.clone()) {
                if meta.size() == 0 {
                    return Ok(AddFileResult::Existing);
                }
            }
        }

        // Check mtime if not ignoring it
        if !ignore_mtime {
            if let Some(meta) = open_file.as_ref().map(|x| x.meta.clone()) {
                let mtime = meta
                    .mtime()
                    .map(|t| DateTime::<Utc>::from(t).with_timezone(&Local));
                if meta.size() == file.meta.size && mtime == file.meta.mtime {
                    debug!(
                        "file {} exists with suitable size and mtime, accepting it!",
                        name.display()
                    );
                    self.matched_size += file.meta.size;
                    return Ok(AddFileResult::Existing);
                }
            }
        }

        let file_idx = self.names.len();
        self.names.push(name);
        let mut file_pos = 0;
        let mut has_unmatched = false;

        // Only open the file once for matching blobs
        let mut open = if let Some(ref file) = open_file
            && dest_be.bias() == RestoreBias::PreferDownloads
        {
            Some(
                file.be
                    .clone()
                    .reader()
                    .ok_or(RusticError::new(
                        ErrorKind::Backend,
                        "Backend does not support reading!",
                    ))?
                    .open_read_start(&file.path)
                    .map_err(|err| {
                        RusticError::with_source(
                            ErrorKind::InputOutput,
                            "Failed to open file for verification",
                            err,
                        )
                    })?,
            )
        } else {
            None
        };

        for id in file.content.iter().flatten() {
            let ie = repo.get_index_entry(id)?;
            let bl = BlobLocation {
                offset: ie.offset,
                length: ie.length,
                uncompressed_length: ie.uncompressed_length,
            };

            let mut matches = false;
            let length: u64 = bl.data_length().into();
            if let Some(open) = open.as_mut() {
                if id.blob_matches_reader(length, open) {
                    matches = true;
                }
            }

            let blob_location = self.r.entry((ie.pack, bl)).or_default();
            blob_location.push(FileLocation {
                file_idx,
                file_start: file_pos,
                matches,
            });

            if matches {
                self.matched_size += length;
            } else {
                self.restore_size += length;
                has_unmatched = true;
            }

            file_pos += length;
        }

        // For backends that don’t support random writes, mark all segments unmatched
        if has_unmatched && !dest_be.supports_random() {
            for id in file.content.iter().flatten() {
                let ie = repo.get_index_entry(id)?;
                for ((pack_id, _), file_locs) in self.r.iter_mut() {
                    if *pack_id == ie.pack {
                        for fl in file_locs.iter_mut() {
                            fl.matches = false;
                        }
                    }
                }
            }
        }

        self.file_lengths.push(file_pos);

        if !has_unmatched && open_file.is_some() {
            Ok(AddFileResult::Verified)
        } else {
            Ok(AddFileResult::Modify)
        }
    }

    /// Get a list of all pack files needed to perform the restore
    ///
    /// This can be used e.g. to warm-up those pack files before doing the actual restore.
    #[must_use]
    pub fn to_packs(&self) -> Vec<PackId> {
        self.r
            .iter()
            // filter out packs which we need
            .filter(|(_, fls)| fls.iter().all(|fl| !fl.matches))
            .map(|((pack, _), _)| *pack)
            .dedup()
            .collect()
    }
}
