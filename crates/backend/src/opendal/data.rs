use crate::opendal::backend::OpenDALBackend;
use crate::opendal::handle::OpenDALHandle;
use crate::opendal::iterator::OpenDALLister;
use chrono::{DateTime, Local, Utc};
use rustic_core::{
    DataFile, DataFilterOptions, DataLister, DataLocation, ExtMetadata, Metadata, UsageStat,
    VfsReader, VfsWriter,
};
use std::io;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use uuid::Uuid;

pub fn path_to_str(p: &Path, is_dir: bool) -> String {
    let mut r = String::from(p.to_str().unwrap());
    if is_dir && !r.ends_with("/") {
        r += "/"
    } else if !is_dir && r.ends_with("/") {
        r = r.strip_suffix("/").unwrap_or(&r).to_string()
    }
    r.replace("\\", "/") // *** fix for windows-style directories
}

pub struct OpenDALWrite {
    writer: opendal::blocking::StdWriter,
}

impl Write for OpenDALWrite {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.writer.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

impl VfsReader for OpenDALBackend {
    fn get_id(&self) -> Uuid {
        self.id
    }

    fn get_usage(&self) -> Option<io::Result<UsageStat>> {
        None
    }

    fn get_metadata(&self, item: &Path) -> io::Result<Option<ExtMetadata>> {
        let path = path_to_str(item, false);
        if !self.operator.exists(&path)? {
            return Ok(None);
        }
        let meta = self.operator.stat(&path)?;
        let x_meta = ExtMetadata {
            path: item.to_path_buf(),
            is_dir: meta.is_dir(),
            mtime: meta
                .last_modified()
                .map(SystemTime::from)
                .map(DateTime::<Utc>::from)
                .map(|x| x.with_timezone(&Local)),
            size: meta.content_length(),
            can_write: true,
            ..Default::default()
        };
        Ok(Some(x_meta))
    }

    fn read_dir(&self, item: &Path, recursive: bool) -> io::Result<Arc<dyn DataLister>> {
        let st = path_to_str(item, true);
        let ret = OpenDALLister::new(self.operator.clone(), &st, None, false, recursive)?;
        Ok(Arc::new(ret))
    }

    fn get_existing(&self, item: &Path) -> io::Result<Option<DataFile>> {
        match self.get_metadata(item)? {
            Some(meta) => {
                let handle = OpenDALHandle::new(self.operator.clone(), item, meta.is_dir);
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

impl VfsWriter for OpenDALBackend {
    fn remove_dir(&self, dirname: &Path) -> io::Result<()> {
        self.operator.remove_all(&path_to_str(dirname, true))?;
        Ok(())
    }

    fn remove_file(&self, filename: &Path) -> io::Result<()> {
        self.operator.delete(&path_to_str(filename, false))?;
        Ok(())
    }

    fn create_dir(&self, item: &Path) -> io::Result<()> {
        self.operator.create_dir(&path_to_str(item, true))?;
        Ok(())
    }

    fn set_times(&self, _item: &Path, _meta: &Metadata) -> io::Result<()> {
        Ok(())
    }

    fn set_length(&self, item: &Path, size: u64) -> io::Result<()> {
        if size != 0 {
            Err(ErrorKind::Unsupported.into())
        } else {
            self.operator
                .write(&path_to_str(item, false), Vec::<u8>::new())?;
            Ok(())
        }
    }

    fn move_to(&self, old: &Path, new: &Path) -> io::Result<()> {
        // Check to see if the current spot is a directory or file.
        let is_dir = self
            .get_existing(old)?
            .map(|x| x.metadata().is_dir)
            .ok_or(io::Error::from(ErrorKind::NotFound))?;
        let src = path_to_str(old, is_dir);
        let dst = path_to_str(new, is_dir);
        self.operator.rename(&src, &dst)?;
        Ok(())
    }

    fn copy_to(&self, old: &Path, new: &Path) -> io::Result<()> {
        let is_dir = self
            .get_existing(old)?
            .map(|x| x.metadata().is_dir)
            .ok_or(io::Error::from(ErrorKind::NotFound))?;
        let src = path_to_str(old, is_dir);
        let dst = path_to_str(new, is_dir);
        self.operator.copy(&src, &dst)?;
        Ok(())
    }
}

impl DataLocation for OpenDALBackend {
    fn read_dir_filtered(
        &self,
        item: &Path,
        opts: Option<DataFilterOptions>,
        recursive: bool,
    ) -> io::Result<Arc<dyn DataLister>> {
        let path = path_to_str(item, true);
        let ret = OpenDALLister::new(self.operator.clone(), &path, opts, false, recursive)?;
        Ok(Arc::new(ret))
    }

    fn get_backup_abs(&self, item: &Path) -> PathBuf {
        item.to_path_buf() // *** the path is already absolute.
    }

    fn supports_random(&self) -> bool {
        false
    }
}
