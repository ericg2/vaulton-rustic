use rustic_core::DataRead;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

pub struct LocalReader {
    file: File,
    path: PathBuf,
}

impl LocalReader {
    pub fn read_file(path: impl AsRef<Path>) -> io::Result<Box<dyn DataRead>> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .append(false)
            .open(&path)?;
        let ret = LocalReader { path, file };
        Ok(Box::new(ret))
    }
}

impl DataRead for LocalReader {
    // fn path(&self) -> PathBuf {
    //     self.path.clone()
    // }
}

impl Read for LocalReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buf)
    }
}

impl Seek for LocalReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.file.seek(pos)
    }
}
