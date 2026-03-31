use crate::backend::node::ExtMetadata;
use crate::{DataFilterOptions, DataLister, Metadata, Node, NodeType, PathList, RusticResult};
use bytes::Bytes;
use std::error::Error;
use std::ffi::OsStr;
use std::fmt::Debug;
use std::fs::File;
use std::io;
use std::io::{ErrorKind, Read, Seek, Write};
use std::num::TryFromIntError;
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// A handle representing operations that can be performed on a file or directory.
///
/// This trait abstracts the underlying storage implementation and exposes
/// capabilities for reading, writing, listing, and modifying items.
///
/// Implementations may selectively support operations; callers should check
/// capability methods (`can_read`, `can_append`, `can_full`) before use.
pub trait FileOpHandle: Send + Sync + 'static {
    /// Returns `true` if the handle supports read access.
    fn can_read(&self) -> bool;

    /// Returns `true` if the handle supports sequential append writes.
    fn can_append(&self) -> bool;

    /// Returns `true` if the handle supports full random read/write access.
    fn can_full(&self) -> bool;

    /// Open a read-only stream.
    ///
    /// # Errors
    /// Returns an error if reading is not supported or the file cannot be opened.
    fn open_read(&self) -> io::Result<Box<dyn DataRead>>;

    /// Open a sequential write stream.
    ///
    /// # Arguments
    /// * `truncate` - If `true`, existing contents are discarded.
    ///
    /// # Errors
    /// Returns an error if writing is not supported or the file cannot be opened.
    fn open_append(&self, truncate: bool) -> io::Result<Box<dyn SeqWrite>>;

    /// Open a random-access read/write stream.
    ///
    /// # Errors
    /// Returns an error if full access is not supported or the file cannot be opened.
    fn open_full(&self) -> io::Result<Box<dyn DataReadWrite>>;

    /// List directory contents NOT recursive.
    ///
    /// # Errors
    /// Returns an error if the directory cannot be read.
    fn read_dir(&self) -> io::Result<Arc<dyn DataLister>>;

    /// Delete the file or directory.
    ///
    /// # Errors
    /// Returns an error if deletion fails or is not permitted.
    fn delete(&self) -> io::Result<()>;

    /// Rename or move the item.
    ///
    /// # Arguments
    /// * `dest` - Destination path.
    ///
    /// # Errors
    /// Returns an error if the operation fails.
    fn rename(&self, dest: &Path) -> io::Result<()>;
}

/// Read-only data stream with seek support.
pub trait DataRead: Read + Seek + Send + Sync + 'static {
    // Returns the underlying path of this stream.
    //fn path(&self) -> PathBuf;
}

/// Sequential write-only stream.
///
/// Implementations must ensure that `close` finalizes the write operation.
pub trait SeqWrite: Write + Send + Sync + 'static {
    /// Finalize and close the stream.
    ///
    /// # Errors
    /// Returns an error if the stream cannot be properly finalized.
    fn close(&mut self) -> io::Result<()>;
}

/// Combined random-access read/write stream.
pub trait DataReadWrite: DataRead + SeqWrite {}


/// Read-only virtual filesystem interface.
///
/// Provides metadata access and directory listing functionality.
pub trait VfsReader: Send + Sync + 'static + Debug {
    /// Retrieves the ID for the [`VfsReader`]. Useful for cross-FS.
    fn get_id(&self) -> Uuid;

    /// Retrieves usage information if applicable.
    fn get_usage(&self) -> Option<io::Result<UsageStat>>;

    /// Retrieve metadata for a path.
    ///
    /// # Returns
    /// `Some(metadata)` if the item exists, otherwise `None`.
    ///
    /// # Errors
    /// Returns an error if metadata cannot be retrieved.
    fn get_metadata(&self, item: &Path) -> io::Result<Option<ExtMetadata>>;

    /// List directory contents.
    ///
    /// # Arguments
    /// * `recursive` - Whether to recurse into subdirectories.
    ///
    /// # Errors
    /// Returns an error if the directory cannot be read.
    fn read_dir(&self, item: &Path, recursive: bool) -> io::Result<Arc<dyn DataLister>>;

    /// Retrieve an existing file entry.
    ///
    /// # Returns
    /// `Some(DataFile)` if the item exists, otherwise `None`.
    ///
    /// # Errors
    /// Returns an error if the lookup fails.
    fn get_existing(&self, item: &Path) -> io::Result<Option<DataFile>>;

    /// Attempt to upgrade to a writable interface.
    ///
    /// # Returns
    /// `Some(&VfsWriter)` if supported, otherwise `None`.
    fn get_writer(&self) -> Option<&dyn VfsWriter> {
        None
    }

    /// Attempt to upgrade to a full data location.
    ///
    /// # Returns
    /// `Some(&DataLocation)` if supported, otherwise `None`.
    fn upgrade(&self) -> Option<&dyn DataLocation> {
        None
    }
}

/// Writable virtual filesystem interface.
///
/// Extends [`VfsReader`] with mutation operations.
pub trait VfsWriter: VfsReader + Send + Sync + 'static + Debug {
    /// Recursively remove a directory and all contents.
    ///
    /// # Errors
    /// Returns an error if removal fails.
    fn remove_dir(&self, dirname: &Path) -> io::Result<()>;

    /// Ensure a file exists, creating it if necessary.
    ///
    /// # Errors
    /// Returns an error if the file cannot be created or accessed.
    fn ensure_file(&self, item: &Path) -> io::Result<DataFile> {
        match self.get_existing(item)? {
            Some(x) => Ok(x),
            None => {
                self.set_length(item, 0)?;
                self.get_existing(item)?.ok_or(ErrorKind::NotFound.into())
            }
        }
    }

    /// Remove a file.
    ///
    /// # Notes
    /// * Must not remove directories.
    /// * Must remove the symlink itself if applicable.
    ///
    /// # Errors
    /// Returns an error if removal fails.
    fn remove_file(&self, filename: &Path) -> io::Result<()>;

    /// Create a directory and any missing parents.
    ///
    /// # Errors
    /// Returns an error if creation fails.
    fn create_dir(&self, item: &Path) -> io::Result<()>;

    /// Set file timestamps.
    ///
    /// # Errors
    /// Returns an error if timestamps cannot be applied.
    fn set_times(&self, item: &Path, meta: &Metadata) -> io::Result<()>;

    /// Set file length.
    ///
    /// # Notes
    /// * Existing files should be resized.
    /// * Missing files should be created.
    ///
    /// # Errors
    /// Returns an error if the operation fails.
    fn set_length(&self, item: &Path, size: u64) -> io::Result<()>;

    /// Move or rename a path.
    ///
    /// # Errors
    /// Returns an error if the operation fails.
    fn move_to(&self, old: &Path, new: &Path) -> io::Result<()>;

    /// Copy a path.
    ///
    /// # Errors
    /// Returns an error if the operation fails.
    fn copy_to(&self, old: &Path, new: &Path) -> io::Result<()>;
}

/// A fully capable data location (read + write + filtering).
pub trait DataLocation: VfsReader + VfsWriter + Send + Sync + 'static + Debug {
    /// List directory contents with filtering.
    ///
    /// # Arguments
    /// * `opts` - Filtering options.
    /// * `recursive` - Whether to recurse.
    ///
    /// # Errors
    /// Returns an error if listing fails.
    fn read_dir_filtered(
        &self,
        item: &Path,
        opts: Option<DataFilterOptions>,
        recursive: bool,
    ) -> io::Result<Arc<dyn DataLister>>;
    
    /// Convert a relative path into a backend-specific absolute path.
    fn get_backup_abs(&self, item: &Path) -> PathBuf;

    /// Retrieve a file only if its size matches.
    ///
    /// # Returns
    /// `Some(DataFile)` if the file exists and matches the size.
    ///
    /// # Errors
    /// Returns an error if metadata retrieval fails.
    fn get_matching_file(&self, item: &Path, size: u64) -> io::Result<Option<DataFile>> {
        match self.get_metadata(item)? {
            Some(meta) if !meta.is_dir && meta.size == size => self.get_existing(item),
            _ => Ok(None),
        }
    }

    /// Returns if the [`DataLocation`] supports random writes.
    fn supports_random(&self) -> bool;
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct UsageStat {
    pub used_bytes: u64,
    pub max_bytes: u64,
}

#[derive(Clone)]
pub struct DataFile {
    handle: Arc<dyn FileOpHandle>,
    pub(crate) meta: ExtMetadata,
}

impl DataFile {
    pub fn from_xmeta<S>(meta: ExtMetadata, be: S) -> Self
    where
        S: FileOpHandle + 'static,
    {
        DataFile {
            meta,
            handle: Arc::new(be),
        }
    }
    pub fn metadata(&self) -> ExtMetadata {
        self.meta.clone()
    }
    pub fn block_write(mut self) -> Self {
        self.meta.can_write = false;
        return self;
    }
    pub fn path(&self) -> PathBuf {
        self.meta.path.clone()
    }
    pub fn name(&self) -> &OsStr {
        self.meta.path.file_name().unwrap_or_default()
    }
    pub fn handle(&self) -> Arc<dyn FileOpHandle> {
        self.handle.clone()
    }
    pub fn node(&self) -> Node {
        Node::new_node(
            self.name(),
            match self.meta.is_dir {
                true => NodeType::Dir,
                false => NodeType::File,
            },
            self.metadata().into(),
        )
    }
}

const DIRECTORY_ERR: &'static str = "Cannot open a directory!";

impl FileOpHandle for DataFile {
    fn can_read(&self) -> bool {
        self.handle.can_read()
    }

    fn can_append(&self) -> bool {
        self.handle.can_append()
    }

    fn can_full(&self) -> bool {
        self.handle.can_full()
    }

    fn open_read(&self) -> io::Result<Box<dyn DataRead>> {
        self.handle.open_read()
    }

    fn open_append(&self, truncate: bool) -> io::Result<Box<dyn SeqWrite>> {
        self.handle.open_append(truncate)
    }

    fn open_full(&self) -> io::Result<Box<dyn DataReadWrite>> {
        self.handle.open_full()
    }

    fn read_dir(&self) -> io::Result<Arc<dyn DataLister>> {
        self.handle.read_dir()
    }

    fn delete(&self) -> io::Result<()> {
        self.handle.delete()
    }

    fn rename(&self, dest: &Path) -> io::Result<()> {
        self.handle.rename(dest)
    }
}

impl From<DataFile> for Node {
    fn from(value: DataFile) -> Self {
        value.node()
    }
}
