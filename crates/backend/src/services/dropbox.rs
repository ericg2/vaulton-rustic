use std::collections::BTreeMap;

use super::RepoConfig;
use rustic_core::RestoreBias;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Hash, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct DropboxConfig {
    pub root: String,
    pub refresh_token: String,
    pub client_id: String,
    pub client_secret: String,
    pub repo_password: String,

    #[serde(default = "RestoreBias::download")]
    pub bias: RestoreBias,
}

const ROOT: &'static str = "root";
const REFRESH_TOKEN: &'static str = "refresh_token";
const CLIENT_ID: &'static str = "client_id";
const CLIENT_SECRET: &'static str = "client_secret";

impl RepoConfig for DropboxConfig {
    fn to_map(self) -> BTreeMap<String, String> {
        let mut map = BTreeMap::new();
        map.insert(ROOT.to_string(), self.root);
        map.insert(REFRESH_TOKEN.to_string(), self.refresh_token);
        map.insert(CLIENT_ID.to_string(), self.client_id);
        map.insert(CLIENT_SECRET.to_string(), self.client_secret);
        return map;
    }

    fn name(&self) -> &'static str {
        "opendal:dropbox"
    }

    fn password(&self) -> String {
        self.repo_password.to_owned()
    }

    fn bias(&self) -> RestoreBias {
        self.bias
    }
}
