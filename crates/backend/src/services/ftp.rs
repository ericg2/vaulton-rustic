use super::RepoConfig;
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use rustic_core::RestoreBias;

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Hash, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct FtpConfig {
    pub endpoint: String,
    pub root: String,
    pub username: String,
    pub password: String,
    pub repo_password: String,

    #[serde(default = "RestoreBias::download")]
    pub bias: RestoreBias,
}

const ENDPOINT: &'static str = "endpoint";
const ROOT: &'static str = "root";
const USER: &'static str = "user";
const PASSWORD: &'static str = "password";

impl RepoConfig for FtpConfig {
    fn to_map(self) -> BTreeMap<String, String> {
        let mut map = BTreeMap::new();
        map.insert(ENDPOINT.to_string(), self.endpoint);
        map.insert(ROOT.to_string(), self.root);
        map.insert(USER.to_string(), self.username);
        map.insert(PASSWORD.to_string(), self.password);
        return map;
    }

    fn name(&self) -> &'static str {
        "opendal:ftp"
    }

    fn password(&self) -> String {
        self.repo_password.to_owned()
    }

    fn bias(&self) -> RestoreBias {
        self.bias
    }
}