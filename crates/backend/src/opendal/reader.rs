use crate::opendal::path_to_str;
use opendal::blocking::{Operator, StdReader};
use rustic_core::DataRead;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::PathBuf;

pub struct OpenDALReader {
    path: PathBuf,
    rdr: StdReader,
}

impl OpenDALReader {
    pub fn new(path: PathBuf, operator: Operator) -> io::Result<Self> {
        let f_path = path_to_str(&path, false);
        let rdr = operator.reader(&f_path)?.into_std_read(..)?;
        Ok(Self { path, rdr })
    }
}

impl DataRead for OpenDALReader {
    // fn path(&self) -> PathBuf {
    //     self.path.clone()
    // }
}

impl Seek for OpenDALReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.rdr.seek(pos)
    }
}

impl Read for OpenDALReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.rdr.read(buf)
    }
}
