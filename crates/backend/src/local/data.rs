use crate::local::backend::LocalBackend;
use crate::local::handle::LocalHandle;
use crate::local::iterator::LocalLister;
use crate::local::writer::LocalWriter;
use filetime::{FileTime, set_symlink_file_times};
use rustic_core::{
    DataFile, DataFilterOptions, DataLister, DataLocation, ExtMetadata, FileOpHandle, Metadata,
    UsageStat, VfsReader, VfsWriter,
};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};
use sysinfo::{Disks, System};
use uuid::Uuid;

impl LocalBackend {
    fn join_force(&self, p: &Path) -> PathBuf {
        rustic_core::join_force(&self.path, p)
    }

    pub fn get_relative(path: &Path, abs: &Path) -> PathBuf {
        match abs.strip_prefix(&path) {
            Ok(rel) => {
                if rel.as_os_str().is_empty() {
                    PathBuf::from("/") // treat same-as-base as root
                } else {
                    PathBuf::from("/").join(rel) // prepend "/" for your VFS style
                }
            }
            Err(_) => PathBuf::from(abs), // fallback: return original path if not under base
        }
    }

    fn raw_metadata(&self, path: &Path) -> io::Result<Option<ExtMetadata>> {
        if !fs::exists(&path)? {
            return Ok(None);
        }
        let meta = fs::metadata(&path)?;
        let x_meta = ExtMetadata {
            path: Self::get_relative(&self.path, &path),
            is_dir: meta.is_dir(),
            mtime: meta.modified().ok().map(|x| x.into()),
            atime: meta.accessed().ok().map(|x| x.into()),
            ctime: meta.created().ok().map(|x| x.into()),
            size: meta.len(),
            can_write: true,
            ..Default::default()
        };
        Ok(Some(x_meta))
    }
}

impl VfsReader for LocalBackend {
    fn get_id(&self) -> Uuid {
        self.id
    }

    fn get_usage(&self) -> Option<io::Result<UsageStat>> {
        let disks = Disks::new_with_refreshed_list();
        let ret = disks
            .iter()
            .find(|x| self.path.starts_with(x.mount_point()))
            .map(|disk| {
                let max_bytes = disk.total_space(); // total bytes
                let free_bytes = disk.available_space(); // free bytes
                let used_bytes = max_bytes - free_bytes;
                UsageStat {
                    used_bytes,
                    max_bytes,
                }
            })
            .ok_or(ErrorKind::Unsupported.into());
        Some(ret)
    }

    fn get_metadata(&self, item: &Path) -> io::Result<Option<ExtMetadata>> {
        let path = self.join_force(item);
        self.raw_metadata(&path)
    }

    fn read_dir(&self, item: &Path, recursive: bool) -> io::Result<Arc<dyn DataLister>> {
        let path = self.join_force(item);
        let ret = LocalLister::new(&self.path, &path, None, recursive)?;
        Ok(Arc::new(ret))
    }

    fn get_existing(&self, item: &Path) -> io::Result<Option<DataFile>> {
        let path = self.join_force(item);
        match self.raw_metadata(&path)? {
            Some(meta) => {
                let handle =
                    LocalHandle::new(self.path.to_owned(), item.to_path_buf(), meta.is_dir);
                let ret = DataFile::from_xmeta(meta, handle);
                Ok(Some(ret))
            }
            None => Ok(None),
        }
    }

    fn get_writer(&self) -> Option<&dyn VfsWriter> {
        Some(self)
    }

    fn upgrade(&self) -> Option<&dyn DataLocation> {
        Some(self)
    }
}

impl VfsWriter for LocalBackend {
    fn remove_dir(&self, dirname: &Path) -> io::Result<()> {
        fs::remove_dir_all(self.join_force(dirname))
    }

    fn remove_file(&self, filename: &Path) -> io::Result<()> {
        fs::remove_file(self.join_force(filename))
    }

    fn create_dir(&self, item: &Path) -> io::Result<()> {
        fs::create_dir_all(self.join_force(item))
    }

    fn set_times(&self, item: &Path, meta: &Metadata) -> io::Result<()> {
        if let Some(mtime) = meta.mtime {
            let atime = meta.atime.unwrap_or(mtime);
            set_symlink_file_times(
                self.join_force(item),
                FileTime::from_system_time(atime.into()),
                FileTime::from_system_time(mtime.into()),
            )?;
        }
        Ok(())
    }

    fn set_length(&self, item: &Path, size: u64) -> io::Result<()> {
        LocalWriter::set_length(&self.join_force(item), size)
    }

    fn move_to(&self, old: &Path, new: &Path) -> io::Result<()> {
        let p_old = self.join_force(old);
        let p_new = self.join_force(new);
        fs::rename(&p_old, &p_new)
    }

    fn copy_to(&self, old: &Path, new: &Path) -> io::Result<()> {
        let p_old = self.join_force(old);
        let p_new = self.join_force(new);
        fs::copy(&p_old, &p_new).map(|_| ())
    }
}

impl DataLocation for LocalBackend {
    fn read_dir_filtered(
        &self,
        item: &Path,
        opts: Option<DataFilterOptions>,
        recursive: bool,
    ) -> io::Result<Arc<dyn DataLister>> {
        let path = self.join_force(item);
        let ret = LocalLister::new(&self.path, &path, opts, recursive)?;
        Ok(Arc::new(ret))
    }

    fn get_backup_abs(&self, item: &Path) -> PathBuf {
        self.join_force(item)
    }

    fn supports_random(&self) -> bool {
        true
    }
}
