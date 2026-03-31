use std::collections::BTreeMap;
use super::RepoConfig;
use serde::{Deserialize, Serialize};
use rustic_core::RestoreBias;

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Hash, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct LocalConfig {
    pub path: String,
    pub repo_password: String,

    #[serde(default = "RestoreBias::download")]
    pub bias: RestoreBias
}

const PATH: &'static str = "path";

impl RepoConfig for LocalConfig {
    fn to_map(self) -> BTreeMap<String, String> {
        BTreeMap::new() // *** no map actually needed.
    }

    fn name(&self) -> &'static str {
        ""
    }

    fn password(&self) -> String {
        self.repo_password.to_owned()
    }

    fn bias(&self) -> RestoreBias {
        self.bias
    }
}