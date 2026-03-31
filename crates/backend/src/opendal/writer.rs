use crate::opendal::path_to_str;
use opendal::blocking::{Operator, StdWriter};
use opendal::options::WriteOptions;
use rustic_core::SeqWrite;
use std::io;
use std::io::Write;
use std::path::PathBuf;

pub struct OpenDALWriter {
    path: PathBuf,
    wtr: StdWriter,
}

impl OpenDALWriter {
    pub fn new(path: PathBuf, operator: Operator, truncate: bool) -> io::Result<Self> {
        let f_path = path_to_str(&path, false);
        let wtr = operator
            .writer_options(
                &f_path,
                WriteOptions {
                    append: !truncate,
                    ..Default::default()
                },
            )?
            .into_std_write();
        Ok(Self { path, wtr })
    }
}

impl SeqWrite for OpenDALWriter {
    fn close(&mut self) -> io::Result<()> {
        self.wtr.close()
    }
}

impl Write for OpenDALWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.wtr.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.wtr.flush()
    }
}
