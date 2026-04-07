//! This module contains [`DataBackendOptions`] and helpers to choose a backend from a given url.
use arbhx_core::VfsBackend;
use derive_setters::Setters;
use std::{collections::BTreeMap, sync::Arc};
use strum_macros::{Display, EnumString};

use rustic_core::{
    DataBackends, ErrorKind, RepositoryBackends, RestoreBias, RusticError, RusticResult,
    WriteBackend,
};

use crate::{ArbhxBackend, util::BackendLocation};

#[cfg(feature = "opendal")]
use crate::opendal::OpenDALBackend;

#[cfg(feature = "rclone")]
use crate::rclone::RcloneBackend;

#[cfg(feature = "rest")]
use crate::rest::RestBackend;

#[cfg(feature = "clap")]
use clap::ValueHint;

/// Options for a backend.
#[cfg_attr(feature = "clap", derive(clap::Parser))]
#[cfg_attr(feature = "merge", derive(conflate::Merge))]
#[derive(Clone, Default, Debug, Setters)]
#[setters(into, strip_option)]
#[non_exhaustive]
pub struct DataBackendOptions {
    /// Repository to use
    pub repository: Option<Arc<dyn VfsBackend>>,

    /// The bias for restoring data.
    pub bias: RestoreBias,
}

impl DataBackendOptions {
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
        // let be = self
        //     .get_backend(self.repository.clone())?
        //     .ok_or_else(|| {
        //         RusticError::new(
        //             ErrorKind::Backend,
        //             "No repository given. Please make sure, that you have set the repository.",
        //         )
        //     })?;
        let be = self.repository.clone().ok_or(RusticError::new(
            ErrorKind::Backend,
            "No repository given. Please make sure, that you have set the repository.",
        ))?;
        let be = ArbhxBackend::new(be.clone())?;
        let ret = Arc::new(be);
        Ok(DataBackends::new(ret.be.clone(), self.bias))
    }

    /// Get the backend for the given repository.
    ///
    /// # Arguments
    ///
    /// * `repo_string` - The repository string to use.
    /// * `options` - Additional options for the backend.
    ///
    /// # Errors
    ///
    /// * If the backend cannot be loaded, an error is returned.
    ///
    /// # Returns
    ///
    /// The backend for the given repository.
    // Allow unused_self, as we want to access this method
    #[allow(clippy::unused_self)]
    fn get_backend(
        &self,
        config: Option<Arc<dyn VfsBackend>>,
    ) -> RusticResult<Option<Arc<dyn WriteBackend>>> {
        match config {
            Some(x) => {
                let be = ArbhxBackend::new(x.clone())?;
                let ret = Arc::new(be);
                Ok(Some(ret))
            }
            None => Ok(None),
        }
    }
}
