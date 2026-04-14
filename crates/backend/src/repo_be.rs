//! This module contains [`BackendOptions`] and helpers to choose a backend from a given url.
use arbhx_core::VfsBackend;
use derive_setters::Setters;
use std::fmt::Debug;
use std::{collections::BTreeMap, sync::Arc};
use std::os::windows::io::HandleOrNull;
use tokio::runtime::{Handle, Runtime};
use rustic_core::{ErrorKind, RepositoryBackends, RusticError, RusticResult, WriteBackend};
use crate::arbhx::ArbhxBackend;

/// Options for a backend.
#[derive(Clone, Default, Debug)]
#[non_exhaustive]
pub struct BackendOptions {
    /// Repository to use
    repository: Option<Arc<dyn VfsBackend>>,

    /// Repository to use as hot storage
    repo_hot: Option<Arc<dyn VfsBackend>>,
}

impl BackendOptions {
    pub fn repository(mut self, be: Arc<dyn VfsBackend>) -> Self {
        self.repository = Some(be);
        self
    }
    pub fn repo_hot(mut self, be: Arc<dyn VfsBackend>) -> Self {
        self.repo_hot = Some(be);
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
    pub fn to_backends(&self, rt: Handle) -> RusticResult<RepositoryBackends> {
        let be = self.get_backend(rt.clone(), self.repository.clone())?.ok_or_else(|| {
            RusticError::new(
                ErrorKind::Backend,
                "No repository given. Please make sure, that you have set the repository.",
            )
        })?;
        let be_hot = self.get_backend(rt.clone(), self.repo_hot.clone())?;
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
        rt: Handle,
        config: Option<Arc<dyn VfsBackend>>,
    ) -> RusticResult<Option<Arc<dyn WriteBackend>>> {
        match config {
            Some(x) => {
                let be = ArbhxBackend::new(rt, x.clone())?;
                let ret = Arc::new(be);
                Ok(Some(ret))
            }
            None => Ok(None),
        }
    }
}
