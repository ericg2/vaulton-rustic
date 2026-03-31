use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub mod b2;
pub mod dropbox;
pub mod ftp;
pub mod g_drive;
pub mod local;
pub mod onedrive;
pub mod s3;

pub use b2::*;
pub use dropbox::*;
pub use ftp::*;
pub use g_drive::*;
pub use local::*;
pub use onedrive::*;
use rustic_core::RestoreBias;
pub use s3::*;

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Hash, Debug)]
#[serde(tag = "Type")]
#[serde(rename_all = "PascalCase")]
pub enum PointSource {
    B2(B2Config),
    Dropbox(DropboxConfig),
    FTP(FtpConfig),
    Google(GDriveConfig),
    OneDrive(OneDriveConfig),
    S3(S3Config),
    Local(LocalConfig),
}

pub trait RepoConfig {
    fn to_map(self) -> BTreeMap<String, String>;
    fn name(&self) -> &'static str;
    fn password(&self) -> String;
    fn bias(&self) -> RestoreBias;
}

impl PointSource {
    pub fn name(&self) -> &str {
        match self {
            PointSource::B2(x) => x.name(),
            PointSource::Dropbox(x) => x.name(),
            PointSource::FTP(x) => x.name(),
            PointSource::Google(x) => x.name(),
            PointSource::OneDrive(x) => x.name(),
            PointSource::S3(x) => x.name(),
            PointSource::Local(x) => &x.path,
        }
    }
    pub fn to_map(self) -> BTreeMap<String, String> {
        match self {
            PointSource::B2(x) => x.to_map(),
            PointSource::Dropbox(x) => x.to_map(),
            PointSource::FTP(x) => x.to_map(),
            PointSource::Google(x) => x.to_map(),
            PointSource::OneDrive(x) => x.to_map(),
            PointSource::S3(x) => x.to_map(),
            PointSource::Local(x) => x.to_map(),
        }
    }
    pub fn get_repo_password(&self) ->String {
        match self {
            PointSource::B2(x) => x.password(),
            PointSource::Dropbox(x) => x.password(),
            PointSource::FTP(x) => x.password(),
            PointSource::Google(x) => x.password(),
            PointSource::OneDrive(x) => x.password(),
            PointSource::S3(x) => x.password(),
            PointSource::Local(x) => x.password(),
        }
    }
    pub fn bias(&self) -> RestoreBias {
        match self {
            PointSource::B2(x) => x.bias(),
            PointSource::Dropbox(x) => x.bias(),
            PointSource::FTP(x) => x.bias(),
            PointSource::Google(x) => x.bias(),
            PointSource::OneDrive(x) => x.bias(),
            PointSource::S3(x) => x.bias(),
            PointSource::Local(x) => x.bias(),
        }
    }
}