//! Module for backend related functionality.
pub(crate) mod cache;
pub(crate) mod decrypt;
pub(crate) mod dry_run;
pub(crate) mod filters;
pub(crate) mod hotcold;
pub(crate) mod ignore;
pub(crate) mod node;
pub(crate) mod warm_up;

use arbhx_core::blocking::{
    DataReadCompat, DataReadSeekCompat, DataWriteCompat, DataWriteSeekCompat, SizedQueryCompat,
    VfsBackendCompat, VfsReaderCompat, VfsWriterCompat,
};
use arbhx_core::{FilterOptions, VfsBackend};
use bytes::Bytes;
use enum_map::Enum;
use log::trace;
#[cfg(test)]
use mockall::mock;
use serde_derive::{Deserialize, Serialize};
use std::path::Path;
use std::{io, io::Read, ops::Deref, path::PathBuf, sync::Arc};

use crate::commands::restore::RestoreBias;
pub(crate) use crate::{
    backend::filters::{DataFilterOptions, DataSaveOptions, RepoFilterOptions},
    backend::node::{Metadata, Node, NodeType},
    error::RusticResult,
    id::Id,
};
use crate::{ErrorKind, PathList, RusticError};

/// [`BackendErrorKind`] describes the errors that can be returned by the various Backends
#[derive(thiserror::Error, Debug, displaydoc::Display)]
#[non_exhaustive]
pub enum BackendErrorKind {
    /// Path is not allowed: `{0:?}`
    PathNotAllowed(PathBuf),
}

pub(crate) type BackendResult<T> = Result<T, BackendErrorKind>;

/// All [`FileType`]s which are located in separated directories
pub const ALL_FILE_TYPES: [FileType; 4] = [
    FileType::Key,
    FileType::Snapshot,
    FileType::Index,
    FileType::Pack,
];

/// Type for describing the kind of file that can occur.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Enum, derive_more::Display)]
pub enum FileType {
    /// Config file
    #[serde(rename = "config")]
    Config,
    /// Index
    #[serde(rename = "index")]
    Index,
    /// Keys
    #[serde(rename = "key")]
    Key,
    /// Snapshots
    #[serde(rename = "snapshot")]
    Snapshot,
    /// Data
    #[serde(rename = "pack")]
    Pack,
}

impl FileType {
    /// Returns the directory name of the file type.
    #[must_use]
    pub const fn dirname(self) -> &'static str {
        match self {
            Self::Config => "config",
            Self::Snapshot => "snapshots",
            Self::Index => "index",
            Self::Key => "keys",
            Self::Pack => "data",
        }
    }

    /// Returns if the file type is cacheable.
    const fn is_cacheable(self) -> bool {
        match self {
            Self::Config | Self::Key | Self::Pack => false,
            Self::Snapshot | Self::Index => true,
        }
    }
}

/// Trait for backends that can read.
///
/// This trait is implemented by all backends that can read data.
pub trait ReadBackend: Send + Sync + 'static {
    /// Returns the location of the backend.
    fn location(&self) -> String;

    /// Lists all files with their size of the given type.
    ///
    /// # Arguments
    ///
    /// * `tpe` - The type of the files to list.
    ///
    /// # Errors
    ///
    /// * If the files could not be listed.
    fn list_with_size(&self, tpe: FileType) -> RusticResult<Vec<(Id, u32)>>;

    /// Lists all files of the given type.
    ///
    /// # Arguments
    ///
    /// * `tpe` - The type of the files to list.
    ///
    /// # Errors
    ///
    /// * If the files could not be listed.
    fn list(&self, tpe: FileType) -> RusticResult<Vec<Id>> {
        Ok(self
            .list_with_size(tpe)?
            .into_iter()
            .map(|(id, _)| id)
            .collect())
    }

    /// Reads full data of the given file.
    ///
    /// # Arguments
    ///
    /// * `tpe` - The type of the file.
    /// * `id` - The id of the file.
    ///
    /// # Errors
    ///
    /// * If the file could not be read.
    fn read_full(&self, tpe: FileType, id: &Id) -> RusticResult<Bytes>;

    /// Reads partial data of the given file.
    ///
    /// # Arguments
    ///
    /// * `tpe` - The type of the file.
    /// * `id` - The id of the file.
    /// * `cacheable` - Whether the file should be cached.
    /// * `offset` - The offset to read from.
    /// * `length` - The length to read.
    ///
    /// # Errors
    ///
    /// * If the file could not be read.
    fn read_partial(
        &self,
        tpe: FileType,
        id: &Id,
        cacheable: bool,
        offset: u32,
        length: u32,
    ) -> RusticResult<Bytes>;

    /// Specify if the backend needs a warming-up of files before accessing them.
    fn needs_warm_up(&self) -> bool {
        false
    }

    /// Warm-up the given file.
    ///
    /// # Arguments
    ///
    /// * `tpe` - The type of the file.
    /// * `id` - The id of the file.
    ///
    /// # Errors
    ///
    /// * If the file could not be read.
    fn warm_up(&self, _tpe: FileType, _id: &Id) -> RusticResult<()> {
        Ok(())
    }
}

/// Trait for Searching in a backend.
///
/// This trait is implemented by all backends that can be searched in.
///
/// # Note
///
/// This trait is used to find the id of a snapshot that contains a given file name.
pub trait FindInBackend: ReadBackend {
    /// Finds the id of the file starting with the given string.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type of the strings.
    ///
    /// # Arguments
    ///
    /// * `tpe` - The type of the file.
    /// * `vec` - The strings to search for.
    ///
    /// # Errors
    ///
    /// * If no id could be found.
    /// * If the id is not unique.
    ///
    /// # Note
    ///
    /// This function is used to find the id of a snapshot.
    fn find_starts_with<T: AsRef<str>>(&self, tpe: FileType, vec: &[T]) -> RusticResult<Vec<Id>> {
        Id::find_starts_with_from_iter(vec, self.list(tpe)?)
    }

    /// Finds the id of the file starting with the given string.
    ///
    /// # Arguments
    ///
    /// * `tpe` - The type of the file.
    /// * `id` - The string to search for.
    ///
    /// # Errors
    ///
    /// * If the string is not a valid hexadecimal string
    /// * If no id could be found.
    /// * If the id is not unique.
    fn find_id(&self, tpe: FileType, id: &str) -> RusticResult<Id> {
        Ok(self.find_ids(tpe, &[id.to_string()])?.remove(0))
    }

    /// Finds the ids of the files starting with the given strings.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The type of the strings.
    ///
    /// # Arguments
    ///
    /// * `tpe` - The type of the file.
    /// * `ids` - The strings to search for.
    ///
    /// # Errors
    ///
    /// * If the string is not a valid hexadecimal string
    /// * If no id could be found.
    /// * If the id is not unique.
    fn find_ids<T: AsRef<str>>(&self, tpe: FileType, ids: &[T]) -> RusticResult<Vec<Id>> {
        ids.iter()
            .map(|id| id.as_ref().parse())
            .collect::<RusticResult<Vec<_>>>()
            .or_else(|err| {
                trace!("no valid IDs given: {err}, searching for ID starting with given strings instead");
                self.find_starts_with(tpe, ids)
            })
    }
}

impl<T: ReadBackend> FindInBackend for T {}

/// Trait for backends that can write.
/// This trait is implemented by all backends that can write data.
pub trait WriteBackend: ReadBackend {
    /// Creates a new backend.
    ///
    /// # Errors
    ///
    /// * If the backend could not be created.
    ///
    /// # Returns
    ///
    /// The result of the creation.
    fn create(&self) -> RusticResult<()> {
        Ok(())
    }

    /// Writes bytes to the given file.
    ///
    /// # Arguments
    ///
    /// * `tpe` - The type of the file.
    /// * `id` - The id of the file.
    /// * `cacheable` - Whether the data can be cached.
    /// * `buf` - The data to write.
    ///
    /// # Errors
    ///
    /// * If the data could not be written.
    ///
    /// # Returns
    ///
    /// The result of the write.
    fn write_bytes(&self, tpe: FileType, id: &Id, cacheable: bool, buf: Bytes) -> RusticResult<()>;

    /// Removes the given file.
    ///
    /// # Arguments
    ///
    /// * `tpe` - The type of the file.
    /// * `id` - The id of the file.
    /// * `cacheable` - Whether the file is cacheable.
    ///
    /// # Errors
    ///
    /// * If the file could not be removed.
    ///
    /// # Returns
    ///
    /// The result of the removal.
    fn remove(&self, tpe: FileType, id: &Id, cacheable: bool) -> RusticResult<()>;
}

#[cfg(test)]
mock! {
    pub(crate) Backend {}

    impl ReadBackend for Backend{
        fn location(&self) -> String;
        fn list_with_size(&self, tpe: FileType) -> RusticResult<Vec<(Id, u32)>>;
        fn read_full(&self, tpe: FileType, id: &Id) -> RusticResult<Bytes>;
        fn read_partial(
            &self,
            tpe: FileType,
            id: &Id,
            cacheable: bool,
            offset: u32,
            length: u32,
        ) -> RusticResult<Bytes>;
    }

    impl WriteBackend for Backend {
        fn create(&self) -> RusticResult<()>;
        fn write_bytes(&self, tpe: FileType, id: &Id, cacheable: bool, buf: Bytes) -> RusticResult<()>;
        fn remove(&self, tpe: FileType, id: &Id, cacheable: bool) -> RusticResult<()>;
    }
}

impl WriteBackend for Arc<dyn WriteBackend> {
    fn create(&self) -> RusticResult<()> {
        self.deref().create()
    }

    fn write_bytes(&self, tpe: FileType, id: &Id, cacheable: bool, buf: Bytes) -> RusticResult<()> {
        self.deref().write_bytes(tpe, id, cacheable, buf)
    }

    fn remove(&self, tpe: FileType, id: &Id, cacheable: bool) -> RusticResult<()> {
        self.deref().remove(tpe, id, cacheable)
    }
}

impl ReadBackend for Arc<dyn WriteBackend> {
    fn location(&self) -> String {
        self.deref().location()
    }

    fn list_with_size(&self, tpe: FileType) -> RusticResult<Vec<(Id, u32)>> {
        self.deref().list_with_size(tpe)
    }

    fn list(&self, tpe: FileType) -> RusticResult<Vec<Id>> {
        self.deref().list(tpe)
    }

    fn read_full(&self, tpe: FileType, id: &Id) -> RusticResult<Bytes> {
        self.deref().read_full(tpe, id)
    }

    fn read_partial(
        &self,
        tpe: FileType,
        id: &Id,
        cacheable: bool,
        offset: u32,
        length: u32,
    ) -> RusticResult<Bytes> {
        self.deref()
            .read_partial(tpe, id, cacheable, offset, length)
    }
}

impl std::fmt::Debug for dyn WriteBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WriteBackend{{{}}}", self.location())
    }
}

/// Trait for backends that can write to a source.
///
/// This trait is implemented by all backends that can write data to a source.
pub trait WriteSource: Clone {
    /// Create a new source.
    ///
    /// # Type Parameters
    ///
    /// * `P` - The type of the path.
    ///
    /// # Arguments
    ///
    /// * `path` - The path of the source.
    /// * `node` - The node information of the source.
    fn create<P: Into<PathBuf>>(&self, path: P, node: Node);

    /// Set the metadata of a source.
    ///
    /// # Type Parameters
    ///
    /// * `P` - The type of the path.
    ///
    /// # Arguments
    ///
    /// * `path` - The path of the source.
    /// * `node` - The node information of the source.
    fn set_metadata<P: Into<PathBuf>>(&self, path: P, node: Node);

    /// Write data to a source at the given offset.
    ///
    /// # Type Parameters
    ///
    /// * `P` - The type of the path.
    ///
    /// # Arguments
    ///
    /// * `path` - The path of the source.
    /// * `offset` - The offset to write at.
    /// * `data` - The data to write.
    fn write_at<P: Into<PathBuf>>(&self, path: P, offset: u64, data: Bytes);
}

/// The backends a repository can be initialized and operated on
///
/// # Note
///
/// This struct is used to initialize a [`Repository`].
///
/// [`Repository`]: crate::Repository
#[derive(Debug, Clone)]
pub struct RepositoryBackends {
    /// The main repository of this [`RepositoryBackends`].
    repository: Arc<dyn WriteBackend>,

    /// The hot repository of this [`RepositoryBackends`].
    repo_hot: Option<Arc<dyn WriteBackend>>,
}

impl RepositoryBackends {
    /// Creates a new [`RepositoryBackends`].
    ///
    /// # Arguments
    ///
    /// * `repository` - The main repository of this [`RepositoryBackends`].
    /// * `repo_hot` - The hot repository of this [`RepositoryBackends`].
    pub fn new(repository: Arc<dyn WriteBackend>, repo_hot: Option<Arc<dyn WriteBackend>>) -> Self {
        Self {
            repository,
            repo_hot,
        }
    }

    /// Returns the repository of this [`RepositoryBackends`].
    #[must_use]
    pub fn repository(&self) -> Arc<dyn WriteBackend> {
        self.repository.clone()
    }

    /// Returns the hot repository of this [`RepositoryBackends`].
    #[must_use]
    pub fn repo_hot(&self) -> Option<Arc<dyn WriteBackend>> {
        self.repo_hot.clone()
    }
}

#[derive(Clone)]
pub struct DataBackends {
    data: Arc<dyn VfsBackendCompat>,
    bias: RestoreBias,
    random: bool,
}

#[derive(Clone)]
pub struct BackupQuery {
    pub(crate) path: PathBuf,
    pub(crate) be: Arc<dyn VfsBackendCompat>,
    pub(crate) inner: Arc<dyn SizedQueryCompat>,
}

#[derive(Clone, Debug)]
pub struct DataFile {
    pub path: PathBuf,
    pub meta: arbhx_core::Metadata,
    pub be: Arc<dyn VfsBackendCompat>,
}

impl From<arbhx_core::Metadata> for Metadata {
    fn from(value: arbhx_core::Metadata) -> Self {
        Self {
            mode: None,
            mtime: value.mtime().map(|x|x.into()),
            atime: value.atime().map(|x|x.into()),
            ctime: None,
            uid: None,
            gid: None,
            user: None,
            group: None,
            inode: 0,
            device_id: 0,
            size: value.size(),
            links: 0,
            x_attrs: vec![],
        }
    }
}

impl DataFile {
    pub fn new(path: &Path, meta: arbhx_core::Metadata, be: Arc<dyn VfsBackendCompat>) -> Self {
        Self {
            path: path.to_path_buf(),
            meta,
            be,
        }
    }

    pub fn node(&self) -> Node {
        Node::new_node(
            self.meta.name(),
            match self.meta.is_dir() {
                true => NodeType::Dir,
                false => NodeType::File,
            },
            self.metadata().into(),
        )
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn metadata(&self) -> arbhx_core::Metadata {
        self.meta.clone()
    }

    pub fn open_read(&self) -> RusticResult<Box<dyn DataReadCompat>> {
        self.be
            .clone()
            .reader()
            .ok_or(RusticError::new(
                ErrorKind::Backend,
                "Reading is not supported",
            ))?
            .open_read_start(&self.path)
            .map_err(|e| {
                RusticError::with_source(ErrorKind::InputOutput, "Backend failed to open read", e)
            })
    }
    pub fn open_read_full(&self) -> RusticResult<Box<dyn DataReadSeekCompat>> {
        self.be
            .clone()
            .reader()
            .ok_or(RusticError::new(
                ErrorKind::Backend,
                "Reading is not supported",
            ))?
            .open_read_seek(&self.path)
            .map_err(|e| {
                RusticError::with_source(ErrorKind::InputOutput, "Backend failed to open read", e)
            })
    }
    pub fn open_write(&self, overwrite: bool) -> RusticResult<Box<dyn DataWriteCompat>> {
        self.be
            .clone()
            .writer()
            .ok_or(RusticError::new(
                ErrorKind::Backend,
                "Writing is not supported",
            ))?
            .open_write(&self.path, overwrite)
            .map_err(|e| {
                RusticError::with_source(ErrorKind::InputOutput, "Backend failed to open write", e)
            })
    }
    pub fn open_write_full(&self) -> RusticResult<Box<dyn DataWriteSeekCompat>> {
        self.be
            .clone()
            .writer_seek()
            .ok_or(RusticError::new(
                ErrorKind::Backend,
                "Seek write is not supported",
            ))?
            .open_write_seek(&self.path)
            .map_err(|e| {
                RusticError::with_source(ErrorKind::InputOutput, "Backend failed to open write", e)
            })
    }
}

impl DataBackends {
    pub fn new(data: Arc<dyn VfsBackendCompat>, bias: RestoreBias) -> Self {
        Self { random: data.clone().writer_seek().is_some(), data, bias }
    }

    fn reader(&self) -> RusticResult<Arc<dyn VfsReaderCompat>> {
        self.data
            .clone()
            .reader()
            .ok_or(RusticError::new(ErrorKind::Backend, "Read not supported"))
    }

    fn writer(&self) -> RusticResult<Arc<dyn VfsWriterCompat>> {
        self.data
            .clone()
            .writer()
            .ok_or(RusticError::new(ErrorKind::Backend, "Write not supported"))
    }

    #[must_use]
    pub fn repository(&self) -> Arc<dyn VfsBackendCompat> {
        self.data.clone()
    }

    pub fn get_matching_file(
        &self,
        path: impl AsRef<Path>,
        size: u64,
    ) -> RusticResult<Option<DataFile>> {
        let path = path.as_ref();
        let ret = self.reader()?.get_metadata(path).map_err(|e| {
            RusticError::with_source(ErrorKind::InputOutput, "Failed to get data", e)
        })?;
        if let Some(x) = ret {
            if x.size() == size {
                let file = DataFile::new(path, x, self.data.clone());
                return Ok(Some(file));
            }
        }
        Ok(None)
    }

    pub fn get_existing(&self, path: impl AsRef<Path>) -> RusticResult<DataFile> {
        let path = path.as_ref();
        let ret = self.reader()?.get_metadata(path).map_err(|e| {
            RusticError::with_source(ErrorKind::InputOutput, "Failed to get data", e)
        })?;
        match ret {
            Some(x) => Ok(DataFile::new(path, x, self.data.clone())),
            None => Err(RusticError::new(ErrorKind::InputOutput, "Item not found")),
        }
    }

    pub fn ensure_file(&self, path: impl AsRef<Path>) -> RusticResult<DataFile> {
        let path = path.as_ref();
        let ret = self.reader()?.get_metadata(path).map_err(|e| {
            RusticError::with_source(ErrorKind::InputOutput, "Failed to get data", e)
        })?;
        match ret {
            Some(x) => Ok(DataFile::new(path, x, self.data.clone())),
            None => {
                // 4-7-26: Attempt to create the file!
                self.writer()?.set_length(path, 0).map_err(|err| {
                    RusticError::with_source(ErrorKind::InputOutput, "Failed to set length!", err)
                })?;
                self.get_existing(path)
            }
        }
    }

    pub fn bias(&self) -> RestoreBias {
        self.bias
    }

    pub(crate) fn prepare_backup(
        &self,
        path: impl AsRef<Path>,
        opts: FilterOptions,
    ) -> RusticResult<BackupQuery> {
        let path = path.as_ref();
        let inner = self
            .data
            .clone()
            .reader()
            .ok_or(RusticError::new(
                ErrorKind::Backend,
                "Failed to prepare backup. Source does not support reading.",
            ))?
            .list(path, Some(opts), true, false)
            .map_err(|err| {
                RusticError::with_source(
                    ErrorKind::Backend,
                    "Failed to prepare backup. Please check all file paths.",
                    err,
                )
            })?;
        Ok(BackupQuery {
            path: path.to_path_buf(),
            be: self.data.clone(),
            inner,
        })
    }

    pub fn supports_random(&self) -> bool {
        self.random
    }
}
