//! This module contains [`DataBackendOptions`] and helpers to choose a backend from a given url.
use arbhx_core::VfsBackend;
use derive_setters::Setters;
use std::{collections::BTreeMap, sync::Arc};
use strum_macros::{Display, EnumString};

use rustic_core::{
    DataBackends, ErrorKind, RepositoryBackends, RestoreBias, RusticError, RusticResult,
    WriteBackend,
};

use crate::{util::BackendLocation};

#[cfg(feature = "opendal")]
use crate::opendal::OpenDALBackend;

#[cfg(feature = "rclone")]
use crate::rclone::RcloneBackend;

#[cfg(feature = "rest")]
use crate::rest::RestBackend;

#[cfg(feature = "clap")]
use clap::ValueHint;
use crate::arbhx::ArbhxBackend;

/// Options for a backend.
#[cfg_attr(feature = "clap", derive(clap::Parser))]
#[cfg_attr(feature = "merge", derive(conflate::Merge))]
#[derive(Clone, Default, Debug)]
#[non_exhaustive]
pub struct DataBackendOptions {
    /// Repository to use
    repository: Option<Arc<dyn VfsBackend>>,

    /// The bias for restoring data.
    bias: RestoreBias,
}

impl DataBackendOptions {
    pub fn repository(mut self, be: Arc<dyn VfsBackend>) -> Self {
        self.repository = Some(be);
        self
    }
    pub fn bias(mut self, bias: RestoreBias) -> Self {
        self.bias = bias;
        self
    }
    /// Convert the options to backends.
    ///
    /// # Errors
    ///
    /// If the repository is not given, an error is returned.
    ///
    /// # Returns
    ///
    /// The backends for the repository.
    pub fn to_backends(&self) -> RusticResult<DataBackends> {
        let be = self.repository.clone().ok_or(RusticError::new(
            ErrorKind::Backend,
            "No repository given. Please make sure, that you have set the repository.",
        ))?;
        let be = ArbhxBackend::new(be.clone())?;
        let ret = Arc::new(be);
        Ok(DataBackends::new(ret.be.clone(), self.bias))
    }
}
