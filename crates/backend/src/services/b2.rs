use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use rustic_core::RestoreBias;
use super::RepoConfig;

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Hash, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct B2Config {
        pub root: String,
        pub application_key: String,
        pub application_key_id: String,
        pub bucket: String,
        pub bucket_id: String,
        pub repo_password: String,
    
    #[serde(default = "RestoreBias::upload")]
    pub bias: RestoreBias,
}

const ROOT: &'static str = "root";
const APP_KEY: &'static str = "application_key";
const APP_ID: &'static str = "application_key_id";
const BUCKET_NAME: &'static str = "bucket";
const BUCKET_ID: &'static str = "bucket_id";

impl RepoConfig for B2Config {
    fn to_map(self) -> BTreeMap<String, String> {
        let mut map = BTreeMap::new();
        map.insert(ROOT.to_string(), self.root);
        map.insert(APP_KEY.to_string(), self.application_key);
        map.insert(APP_ID.to_string(), self.application_key_id);
        map.insert(BUCKET_NAME.to_string(), self.bucket);
        map.insert(BUCKET_ID.to_string(), self.bucket_id);
        return map;
    }

    fn name(&self) -> &'static str {
        "opendal:b2"
    }

    fn password(&self) -> String {
        self.repo_password.to_owned()
    }

    fn bias(&self) -> RestoreBias { self.bias }
}