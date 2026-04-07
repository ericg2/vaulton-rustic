//! This module contains [`BackendOptions`] and helpers to choose a backend from a given url.
use arbhx_core::VfsBackend;
use derive_setters::Setters;
use std::fmt::Debug;
use std::{collections::BTreeMap, sync::Arc};

use rustic_core::{ErrorKind, RepositoryBackends, RusticError, RusticResult, WriteBackend};

use crate::ArbhxBackend;

/// Options for a backend.
#[derive(Clone, Default, Debug, Setters)]
#[setters(into, strip_option)]
#[non_exhaustive]
pub struct BackendOptions {
    /// Repository to use
    pub repository: Option<Arc<dyn VfsBackend>>,

    /// Repository to use as hot storage
    pub repo_hot: Option<Arc<dyn VfsBackend>>,
}

impl BackendOptions {
    /// Convert the options to backends.
    ///
    /// # Errors
    ///
    /// If the repository is not given, an error is returned.
    ///
    /// # Returns
    ///
    /// The backends for the repository.
    pub fn to_backends(&self) -> RusticResult<RepositoryBackends> {
        let be = self.get_backend(self.repository.clone())?.ok_or_else(|| {
            RusticError::new(
                ErrorKind::Backend,
                "No repository given. Please make sure, that you have set the repository.",
            )
        })?;
        let be_hot = self.get_backend(self.repo_hot.clone())?;
        Ok(RepositoryBackends::new(be, be_hot))
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
//
// /// Trait which can be implemented to choose a backend from a backend type, a backend path and options given as `HashMap`.
// pub trait BackendChoice {
//     /// Init backend from a path and options.
//     ///
//     /// # Arguments
//     ///
//     /// * `path` - The path to create that points to the backend.
//     /// * `options` - additional options for creating the backend
//     ///
//     /// # Errors
//     ///
//     /// * If the backend is not supported.
//     fn to_backend(
//         &self,
//         location: BackendLocation,
//         options: Option<BTreeMap<String, String>>,
//     ) -> RusticResult<Arc<dyn WriteBackend>>;
// }
