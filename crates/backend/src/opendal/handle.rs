use crate::opendal::writer::OpenDALWriter;
use crate::opendal::{OpenDALLister, OpenDALReader, path_to_str};
use opendal::blocking::Operator;
use rustic_core::{DataLister, DataRead, DataReadWrite, FileOpHandle, SeqWrite};
use std::io;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct OpenDALHandle {
    pub(crate) operator: Operator,
    pub(crate) path: PathBuf,
    pub(crate) is_dir: bool,
}

impl OpenDALHandle {
    pub fn new(operator: Operator, path: &Path, is_dir: bool) -> Self {
        Self {
            operator,
            path: path.into(),
            is_dir,
        }
    }
}

impl FileOpHandle for OpenDALHandle {
    fn can_read(&self) -> bool {
        true
    }

    fn can_append(&self) -> bool {
        true
    }

    fn can_full(&self) -> bool {
        false // *** random offset NOT supported!
    }

    fn open_read(&self) -> io::Result<Box<dyn DataRead>> {
        let ret = OpenDALReader::new(self.path.clone(), self.operator.clone())?;
        Ok(Box::new(ret))
    }

    fn open_append(&self, truncate: bool) -> io::Result<Box<dyn SeqWrite>> {
        let ret = OpenDALWriter::new(self.path.clone(), self.operator.clone(), truncate)?;
        Ok(Box::new(ret))
    }

    fn open_full(&self) -> io::Result<Box<dyn DataReadWrite>> {
        Err(ErrorKind::Unsupported.into()) // *** OpenDAL does not support this mode!
    }

    fn read_dir(&self) -> io::Result<Arc<dyn DataLister>> {
        let path = path_to_str(&self.path, true);
        let ret = OpenDALLister::new(self.operator.clone(), &path, None, false, false)?;
        Ok(Arc::new(ret))
    }

    fn delete(&self) -> io::Result<()> {
        let path = path_to_str(&self.path, self.is_dir);
        if self.is_dir {
            self.operator.remove_all(&path)?;
        } else {
            self.operator.delete(&path)?;
        }
        Ok(())
    }

    fn rename(&self, dest: &Path) -> io::Result<()> {
        let src = path_to_str(&self.path, self.is_dir);
        let dst = path_to_str(dest, self.is_dir);
        self.operator.rename(&src, &dst)?;
        Ok(())
    }
}
