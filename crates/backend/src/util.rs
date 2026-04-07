use chrono::{DateTime, Local, Utc};
use rustic_core::{ErrorKind, RusticError, RusticResult};
use std::path::{Component, Path, PathBuf};
use std::time::SystemTime;

/// A backend location. This is a string that represents the location of the backend.
#[derive(PartialEq, Eq, Debug, Clone)]
pub struct BackendLocation(String);

impl std::ops::Deref for BackendLocation {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<str> for BackendLocation {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for BackendLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)?;
        Ok(())
    }
}

pub fn strip_all<'a>(st: &'a str, p: &'a str) -> &'a str {
    let x = st.strip_prefix(p).unwrap_or(st);
    x.strip_suffix(p).unwrap_or(x)
}