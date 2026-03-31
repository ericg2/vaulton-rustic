//! This module contains [`DataBackendOptions`] and helpers to choose a backend from a given url.
use derive_setters::Setters;
use std::{collections::BTreeMap, sync::Arc};
use strum_macros::{Display, EnumString};

use rustic_core::{DataBackends, DataLocation, ErrorKind, RepositoryBackends, RestoreBias, RusticError, RusticResult, WriteBackend};

use crate::{
    local::LocalBackend,
    util::{BackendLocation, data_location_to_type_and_path},
};

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
#[derive(Clone, Default, Debug, serde::Deserialize, serde::Serialize, Setters)]
#[serde(default, rename_all = "kebab-case", deny_unknown_fields)]
#[setters(into, strip_option)]
#[non_exhaustive]
pub struct DataBackendOptions {
    /// Repository to use
    #[cfg_attr(
        feature = "clap",
        clap(short, long, global = true, visible_alias = "repo", env = "RUSTIC_REPOSITORY", value_hint = ValueHint::DirPath)
    )]
    #[cfg_attr(feature = "merge", merge(strategy = conflate::option::overwrite_none))]
    pub repository: Option<String>,
    
    /// Other options for this repository (hot and cold part)
    #[cfg_attr(feature = "clap", clap(skip))]
    #[cfg_attr(feature = "merge", merge(strategy = conflate::btreemap::append_or_ignore))]
    pub options: BTreeMap<String, String>,

    /// The [`RestoreBias`] for restoring data.
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
        let be = self
            .get_backend(self.repository.as_ref(), self.options.clone())?
            .ok_or_else(|| {
                RusticError::new(
                    ErrorKind::Backend,
                    "No repository given. Please make sure, that you have set the repository.",
                )
            })?;
        Ok(DataBackends::new(be, self.bias))
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
        repo_string: Option<&String>,
        options: BTreeMap<String, String>,
    ) -> RusticResult<Option<Arc<dyn DataLocation>>> {
        repo_string
            .map(|string| {
                let (be_type, location) = data_location_to_type_and_path(string)?;
                be_type
                    .to_backend(location.clone(), options.into())
                    .map_err(|err| {
                        err
                        .prepend_guidance_line("Could not load the backend `{name}` at `{location}`. Please check the given backend and try again.")
                        .attach_context("name", be_type.to_string())
                        .attach_context("location", location.to_string())
                    })
            })
            .transpose()
    }
}

/// The supported backend types.
///
/// Currently supported types are "local", "rclone", "rest", "opendal"
///
/// # Notes
///
/// If the url is a windows path, the type will be "local".
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumString, Display)]
pub enum SupportedDataBackend {
    /// A local backend
    #[strum(serialize = "local", to_string = "Local Backend")]
    Local,

    #[cfg(feature = "opendal")]
    /// An openDAL backend (general)
    #[strum(serialize = "opendal", to_string = "openDAL Backend")]
    OpenDAL,
}

impl SupportedDataBackend {
    fn to_backend(
        &self,
        location: BackendLocation,
        options: Option<BTreeMap<String, String>>,
    ) -> RusticResult<Arc<dyn DataLocation>> {
        let options = options.unwrap_or_default();
        Ok(match self {
            Self::Local => Arc::new(LocalBackend::new(location, options)?),
            #[cfg(feature = "opendal")]
            Self::OpenDAL => Arc::new(OpenDALBackend::new(location, options)?),
        })
    }
}