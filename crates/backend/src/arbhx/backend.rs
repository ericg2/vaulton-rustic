use arbhx::blocking::*;
use bytes::Bytes;
use log::{error, trace};
use rustic_core::{
    ALL_FILE_TYPES, ErrorKind, FileType, Id, ReadBackend, RusticError, RusticResult, WriteBackend,
};
use std::io;
use std::io::{Read, SeekFrom, Write};
use typed_path::UnixPathBuf;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct ArbhxBackend {
    pub(crate) id: Uuid,
    pub operator: DataOperator,
}

impl ArbhxBackend {
    /// Return a path for the given file type and id.
    ///
    /// # Arguments
    ///
    /// * `tpe` - The type of the file.
    /// * `id` - The id of the file.
    ///
    /// # Returns
    ///
    /// The path for the given file type and id.
    // Let's keep this for now, as it's being used in the trait implementations.
    #[allow(clippy::unused_self)]
    fn path(&self, tpe: FileType, id: &Id) -> String {
        let hex_id = id.to_hex();
        match tpe {
            FileType::Config => UnixPathBuf::from("config"),
            FileType::Pack => UnixPathBuf::from("data")
                .join(&hex_id[0..2])
                .join(&hex_id[..]),
            _ => UnixPathBuf::from(tpe.dirname()).join(&hex_id[..]),
        }
        .to_string()
    }
}

impl ReadBackend for ArbhxBackend {
    fn location(&self) -> String {
        todo!()
    }

    fn list_with_size(&self, tpe: FileType) -> RusticResult<Vec<(Id, u32)>> {
        trace!("listing tpe: {tpe:?}");
        if tpe == FileType::Config {
            return match self.operator.stat("config") {
                Ok(Some(entry)) => Ok(vec![(
                    Id::default(),
                    entry.size.try_into().map_err(|err| {
                        RusticError::with_source(
                            ErrorKind::Internal,
                            "Parsing content length `{length}` failed",
                            err,
                        )
                            .attach_context("length", entry.size.to_string())
                    })?,
                )]),
                Ok(None) => Ok(Vec::new()),
                Err(err) => Err(err).map_err(|err|
                    RusticError::with_source(
                        ErrorKind::Backend,
                        "Getting Metadata of type `{type}` failed in the backend. Please check if `{type}` exists.",
                        err,
                    )
                        .attach_context("type", tpe.to_string())
                ),
            };
        }

        let path = tpe.dirname().to_string() + "/";
        let ret =self.operator.list(&path, None, true, true).and_then(|x| x.stream()).map_err(|err| {
            RusticError::with_source(
                ErrorKind::Backend,
                "Listing all files of `{type}` in directory `{path}` and their sizes failed in the backend. Please check if the given path is correct.",
                err,
            )
                .attach_context("path", path)
                .attach_context("type", tpe.to_string())
        })?;
        Ok(ret
            .into_iter()
            .filter_map(Result::ok)
            .filter(|x| !x.metadata().is_dir)
            .map(|e| -> RusticResult<(Id, u32)> {
                Ok((
                    e.name().to_str().unwrap().parse()?,
                    e.metadata().size.try_into().map_err(|err| {
                        RusticError::with_source(
                            ErrorKind::Internal,
                            "Parsing content length `{length}` failed",
                            err,
                        )
                        .attach_context("length", e.metadata().size.to_string())
                    })?,
                ))
            })
            .inspect(|r| {
                if let Err(err) = r {
                    error!("Error while listing files: {}", err.display_log());
                }
            })
            .filter_map(RusticResult::ok)
            .collect())
    }

    fn list(&self, tpe: FileType) -> RusticResult<Vec<Id>> {
        trace!("listing tpe: {tpe:?}");
        if tpe == FileType::Config {
            return Ok(
                if self
                    .operator
                    .stat("config")
                    .map_err(|err| {
                        RusticError::with_source(
                            ErrorKind::Backend,
                            "Path `config` does not exist.",
                            err,
                        )
                        .ask_report()
                    })?
                    .is_some()
                {
                    vec![Id::default()]
                } else {
                    Vec::new()
                },
            );
        }

        let path = tpe.dirname().to_string() + "/";
        let ret = self.operator.list(&path, None, true, true).and_then(|x| x.stream()).map_err(|err| {
            RusticError::with_source(
                ErrorKind::Backend,
                "Listing all files of `{type}` in directory `{path}` and their sizes failed in the backend. Please check if the given path is correct.",
                err,
            )
                .attach_context("path", path)
                .attach_context("type", tpe.to_string())
        })?;
        Ok(ret
            .into_iter()
            .filter_map(Result::ok)
            .filter(|x| !x.metadata().is_dir)
            .filter_map(|e| e.name().to_str()?.parse().ok())
            .collect())
    }

    fn read_full(&self, tpe: FileType, id: &Id) -> RusticResult<Bytes> {
        trace!("reading tpe: {tpe:?}, id: {id}");
        let path = self.path(tpe, id);
        let mut buf = vec![];
        self.operator
            .open_read(&path)
            .and_then(|mut x|x.read_to_end(&mut buf))
            .map_err(|err|
                RusticError::with_source(
                    ErrorKind::Backend,
                    "Reading file `{path}` failed in the backend. Please check if the given path is correct.",
                    err,
                )
                .attach_context("path", path)
                .attach_context("type", tpe.to_string())
                .attach_context("id", id.to_string())
        )?;
        Ok(Bytes::from(buf))
    }

    fn read_partial(
        &self,
        tpe: FileType,
        id: &Id,
        cacheable: bool,
        offset: u32,
        length: u32,
    ) -> RusticResult<Bytes> {
        trace!("reading tpe: {tpe:?}, id: {id}, offset: {offset}, length: {length}");
        let path = self.path(tpe, id);
        let mut file = self.operator.open_read(&path).map_err(|err| {
            RusticError::with_source(
                ErrorKind::Backend,
                "Failed to open the file `{path}`. Please check the file and try again.",
                err,
            )
            .attach_context("path", &path)
        })?;
        _ = file.seek(SeekFrom::Start(offset.into())).map_err(|err| {
            RusticError::with_source(
                ErrorKind::Backend,
                "Failed to seek to the position `{offset}` in the file `{path}`. Please check the file and try again.",
                err,
            )
                .attach_context("path", &path)
                .attach_context("offset", offset.to_string())
        })?;
        let mut vec = vec![
            0;
            length.try_into().map_err(|err| {
                RusticError::with_source(
                    ErrorKind::Backend,
                    "Failed to convert length `{length}` to u64.",
                    err,
                )
                .attach_context("length", length.to_string())
                .ask_report()
            })?
        ];
        file.read_exact(&mut vec).map_err(|err| {
            RusticError::with_source(
                ErrorKind::Backend,
                "Failed to read the exact length `{length}` of the file `{path}`. Please check the file and try again.",
                err,
            )
                .attach_context("path", &path)
                .attach_context("length", length.to_string())
        })?;

        Ok(vec.into())
    }
}

impl WriteBackend for ArbhxBackend {
    fn create(&self) -> RusticResult<()> {
        trace!("creating repo at {:?}", self.location());
        for tpe in ALL_FILE_TYPES {
            let path = tpe.dirname().to_string() + "/";
            self.operator.create_dir(&path)       .map_err(|err|
                RusticError::with_source(
                    ErrorKind::Backend,
                    "Creating directory `{path}` failed in the backend `{location}`. Please check if the given path is correct.",
                    err,
                )
                    .attach_context("path", path)
                    .attach_context("location", self.location())
                    .attach_context("type", tpe.to_string())
            )?;
        }
        // TODO: try to parallelize like the original?
        for i in 0u8..=255 {
            let path = UnixPathBuf::from("data")
                .join(hex::encode([i]))
                .to_string_lossy()
                .to_string()
                + "/";
            self.operator.create_dir(&path).map_err(|err|
                RusticError::with_source(
                    ErrorKind::Backend,
                    "Creating directory `{path}` failed in the backend `{location}`. Please check if the given path is correct.",
                    err,
                )
                    .attach_context("path", path)
                    .attach_context("location", self.location())
            )?;
        }
        Ok(())
    }
    fn write_bytes(&self, tpe: FileType, id: &Id, cacheable: bool, buf: Bytes) -> RusticResult<()> {
        trace!("writing tpe: {:?}, id: {}", &tpe, &id);
        let filename = self.path(tpe, id);
        self.operator
            .open_append(&filename, true)
            .and_then(|mut x| {
                x.write(&*buf)?;
                x.flush()?;
                x.close()
            })
            .map_err(|err| {
                RusticError::with_source(
                    ErrorKind::Backend,
                    "Writing file `{path}` failed in the backend. Please check if the given path is correct.",
                    err,
                )
                    .attach_context("path", filename)
                    .attach_context("type", tpe.to_string())
                    .attach_context("id", id.to_string())
        })?;
        Ok(())
    }

    fn remove(&self, tpe: FileType, id: &Id, _cacheable: bool) -> RusticResult<()> {
        trace!("removing tpe: {:?}, id: {}", &tpe, &id);
        let filename = self.path(tpe, id);
        self.operator.remove_file(&filename).map_err(|err| {
            RusticError::with_source(
                ErrorKind::Backend,
                "Deleting file `{path}` failed in the backend. Please check if the given path is correct.",
                err,
            )
                .attach_context("path", filename)
                .attach_context("type", tpe.to_string())
                .attach_context("id", id.to_string())
        })?;
        Ok(())
    }
}
