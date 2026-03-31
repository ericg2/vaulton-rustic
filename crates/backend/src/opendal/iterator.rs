use std::io;
use crate::filters::DataFilter;
use crate::opendal::backend::OpenDALBackend;
use chrono::{DateTime, Local, Utc};
use opendal::blocking::Operator;
use opendal::options::ListOptions;
use opendal::Entry;
use rustic_core::{
    DataFile, DataFilterOptions, DataIterator, DataLister, ErrorKind, ExtMetadata
    , RusticError, RusticResult,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use crate::opendal::handle::OpenDALHandle;

pub struct OpenDALLister {
    pub(crate) operator: Operator,
    pub(crate) path: String,
    pub(crate) options: DataFilterOptions,
    pub(crate) filter: DataFilter,
    pub(crate) enable_size: bool,
    pub(crate) recursive: bool,
}

fn ensure_leading_slash(mut path: PathBuf) -> PathBuf {
    let s = path.to_string_lossy();
    if !s.starts_with('/') {
        let mut new_path = PathBuf::from("/");
        new_path.push(path);
        return new_path;
    }
    path
}

impl OpenDALLister {
    pub(crate) fn new(
        operator: Operator,
        path: &str,
        options: Option<DataFilterOptions>,
        enable_size: bool,
        recursive: bool,
    ) -> io::Result<Self> {
        let options = options.unwrap_or_default();
        let filter = DataFilter::new(&options)?;
        Ok(Self {
            operator,
            filter,
            options,
            enable_size,
            recursive,
            path: path.into(),
        })
    }

    fn get_file(operator: &Operator, entry: Entry) -> DataFile {
        let path = PathBuf::from(entry.path());
        let f_path = ensure_leading_slash(path.clone());
        let meta = entry.metadata();
        let x_meta = ExtMetadata {
            path: f_path.clone(),
            is_dir: meta.is_dir(),
            mtime: meta
                .last_modified()
                .map(SystemTime::from)
                .map(DateTime::<Utc>::from)
                .map(|x| x.with_timezone(&Local)),
            size: meta.content_length(),
            ..Default::default()
        };

        let reader = OpenDALHandle::new(operator.clone(), &path, meta.is_dir());
        DataFile::from_xmeta(x_meta, reader)
    }

    fn get_entry(&self, res: Result<Entry, opendal::Error>) -> io::Result<Option<DataFile>> {
        let file = Self::get_file(&self.operator, res?);
        match self.filter.filter_ok(&file)? {
            true => Ok(Some(file)),
            false => Ok(None),
        }
    }
}

impl DataLister for OpenDALLister {
    fn size(&self) -> io::Result<Option<u64>> {
        Ok(None)
    }

    fn options(&self) -> DataFilterOptions {
        self.options.clone()
    }

    fn get_iter(self: Arc<Self>) -> io::Result<Box<dyn DataIterator>> {
        let iter = self
            .operator
            .lister_options(
                &self.path,
                ListOptions {
                    recursive: self.recursive,
                    ..Default::default()
                },
            )?
            .into_iter()
            .map(move |entry| OpenDALLister::get_entry(&self, entry).transpose())
            .flatten();

        Ok(Box::new(iter))
    }

    fn path(&self) -> &Path {
        Path::new(&self.path)
    }
}
