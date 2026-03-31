use crate::local::writer::LocalWriter;
use crate::local::{LocalLister, LocalReader};
use rustic_core::{DataLister, DataRead, DataReadWrite, FileOpHandle, SeqWrite};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};

pub struct LocalHandle {
    abs: PathBuf,
    path: PathBuf,
    is_dir: bool,
}

impl LocalHandle {
    pub fn new(abs: PathBuf, path: PathBuf, is_dir: bool) -> Self {
        Self { abs, path, is_dir }
    }
}

impl FileOpHandle for LocalHandle {
    fn can_read(&self) -> bool {
        true
    }

    fn can_append(&self) -> bool {
        true
    }

    fn can_full(&self) -> bool {
        true
    }

    fn open_read(&self) -> io::Result<Box<dyn DataRead>> {
        let path = rustic_core::join_force(&self.abs, &self.path);
        LocalReader::read_file(&path)
    }

    fn open_append(&self, truncate: bool) -> io::Result<Box<dyn SeqWrite>> {
        let path = rustic_core::join_force(&self.abs, &self.path);
        LocalWriter::sequential(&path, truncate)
    }

    fn open_full(&self) -> io::Result<Box<dyn DataReadWrite>> {
        let path = rustic_core::join_force(&self.abs, &self.path);
        LocalWriter::full(&path)
    }

    fn read_dir(&self) -> io::Result<Arc<dyn DataLister>> {
        let ret = LocalLister::new(&self.abs, &self.path, None, false)?;
        Ok(Arc::new(ret))
    }

    fn delete(&self) -> io::Result<()> {
        let path = rustic_core::join_force(&self.abs, &self.path);
        if self.is_dir {
            fs::remove_dir_all(&path)
        } else {
            fs::remove_file(&path)
        }
    }

    fn rename(&self, dest: &Path) -> io::Result<()> {
        let path = rustic_core::join_force(&self.abs, &self.path);
        let dst = rustic_core::join_force(&path, dest);
        fs::rename(&path, &dst)
    }
}
