use super::RepoConfig;
use rustic_core::RestoreBias;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Serialize, Deserialize, Eq, Hash, PartialEq, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct S3Config {
    pub root: String,
    pub bucket: String,
    pub endpoint: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub repo_password: String,

    #[serde(default = "RestoreBias::upload")]
    pub bias: RestoreBias,
}

const ROOT: &'static str = "root";
const BUCKET: &'static str = "bucket";
const ENDPOINT: &'static str = "endpoint";
const REGION: &'static str = "region";
const ACCESS_KEY_ID: &'static str = "access_key_id";
const SECRET_ACCESS_KEY: &'static str = "secret_access_key";

impl RepoConfig for S3Config {
    fn to_map(self) -> BTreeMap<String, String> {
        let mut map = BTreeMap::new();
        map.insert(ROOT.to_string(), self.root);
        map.insert(BUCKET.to_string(), self.bucket);
        map.insert(ENDPOINT.to_string(), self.endpoint);
        map.insert(REGION.to_string(), self.region);
        map.insert(ACCESS_KEY_ID.to_string(), self.access_key_id);
        map.insert(SECRET_ACCESS_KEY.to_string(), self.secret_access_key);
        return map;
    }

    fn name(&self) -> &'static str {
        "opendal:s3"
    }

    fn password(&self) -> String {
        self.repo_password.to_owned()
    }

    fn bias(&self) -> RestoreBias {
        self.bias
    }
}
