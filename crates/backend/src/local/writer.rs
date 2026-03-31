use rustic_core::{DataRead, DataReadWrite, SeqWrite};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::{fs, io};

pub struct LocalWriter {
    file: File,
}

impl SeqWrite for LocalWriter {
    fn close(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Write for LocalWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl Read for LocalWriter {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.file.read(buf)
    }
}

impl Seek for LocalWriter {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.file.seek(pos)
    }
}

impl DataRead for LocalWriter {}
impl DataReadWrite for LocalWriter {} // TODO: consolidate into one file!

impl LocalWriter {
    pub fn sequential(path: impl AsRef<Path>, truncate: bool) -> io::Result<Box<dyn SeqWrite>> {
        let file = OpenOptions::new()
            .read(false)
            .append(true)
            .truncate(truncate)
            .open(path.as_ref())?;
        let ret = LocalWriter { file };
        Ok(Box::new(ret))
    }
    pub fn full(path: impl AsRef<Path>) -> io::Result<Box<dyn DataReadWrite>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref())?;
        let ret = LocalWriter { file };
        Ok(Box::new(ret))
    }
    pub fn set_length(path: impl AsRef<Path>, size: u64) -> io::Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path)?
            .set_len(size)?;
        Ok(())
    }
}
