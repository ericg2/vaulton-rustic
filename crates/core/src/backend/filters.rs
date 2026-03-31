use bytesize::ByteSize;
use derive_setters::Setters;
use serde_with::{DisplayFromStr, serde_as};
use crate::LsOptions;

#[serde_as]
#[cfg_attr(feature = "clap", derive(clap::Parser))]
#[cfg_attr(feature = "merge", derive(conflate::Merge))]
#[derive(serde::Deserialize, serde::Serialize, Default, Clone, Copy, Debug, Setters)]
#[serde(default, rename_all = "kebab-case", deny_unknown_fields)]
#[setters(into)]
#[non_exhaustive]
/// [`DataSaveOptions`] describes how entries from a local source will be saved in the repository.
pub struct DataSaveOptions {
    /// Save access time for files and directories
    #[cfg_attr(feature = "clap", clap(long))]
    #[cfg_attr(feature = "merge", merge(strategy = conflate::bool::overwrite_false))]
    pub with_atime: bool,

    /// Don't save device ID for files and directories
    #[cfg_attr(feature = "clap", clap(long))]
    #[cfg_attr(feature = "merge", merge(strategy = conflate::bool::overwrite_false))]
    pub ignore_devid: bool,
}

#[serde_as]
#[cfg_attr(feature = "clap", derive(clap::Parser))]
#[cfg_attr(feature = "merge", derive(conflate::Merge))]
#[derive(serde::Deserialize, serde::Serialize, Default, Clone, Debug, Setters)]
#[serde(default, rename_all = "PascalCase", deny_unknown_fields)]
#[setters(into)]
#[non_exhaustive]
pub struct RepoFilterOptions {
    /// Glob pattern to exclude/include (can be specified multiple times)
    #[cfg_attr(feature = "clap", clap(long, help_heading = "Exclude options"))]
    pub glob: Vec<String>,

    /// Same as --glob pattern but ignores the casing of filenames
    #[cfg_attr(
        feature = "clap",
        clap(long, value_name = "GLOB", help_heading = "Exclude options")
    )]
    pub iglob: Vec<String>,

    /// Read glob patterns to exclude/include from this file (can be specified multiple times)
    #[cfg_attr(
        feature = "clap",
        clap(long, value_name = "FILE", help_heading = "Exclude options")
    )]
    pub glob_file: Vec<String>,

    /// Same as --glob-file ignores the casing of filenames in patterns
    #[cfg_attr(
        feature = "clap",
        clap(long, value_name = "FILE", help_heading = "Exclude options")
    )]
    pub iglob_file: Vec<String>,

    /// recursively list the dir
    #[cfg_attr(feature = "clap", clap(long))]
    pub recursive: bool,
}

impl From<RepoFilterOptions> for LsOptions {
    fn from(value: RepoFilterOptions) -> Self {
        Self {
            glob: value.glob,
            iglob: value.iglob,
            glob_file: value.glob_file,
            iglob_file: value.iglob_file,
            recursive: value.recursive,
        }
    }
}


#[serde_as]
#[cfg_attr(feature = "clap", derive(clap::Parser))]
#[cfg_attr(feature = "merge", derive(conflate::Merge))]
#[derive(serde::Deserialize, serde::Serialize, Default, Clone, Debug, Setters)]
#[serde(default, rename_all = "PascalCase", deny_unknown_fields)]
#[setters(into)]
#[non_exhaustive]
/// [`DataFilterOptions`] allow to filter a local source by various criteria.
pub struct DataFilterOptions {
    /// Glob pattern to exclude/include (can be specified multiple times)
    #[cfg_attr(feature = "clap", clap(long = "glob", value_name = "GLOB"))]
    #[cfg_attr(feature = "merge", merge(strategy = conflate::vec::overwrite_empty))]
    pub globs: Vec<String>,

    /// Same as --glob pattern but ignores the casing of filenames
    #[cfg_attr(feature = "clap", clap(long = "iglob", value_name = "GLOB"))]
    #[cfg_attr(feature = "merge", merge(strategy = conflate::vec::overwrite_empty))]
    pub ignore_globs: Vec<String>,

    // /// Read glob patterns to exclude/include from this file (can be specified multiple times)
    // #[cfg_attr(feature = "clap", clap(long = "glob-file", value_name = "FILE"))]
    // #[cfg_attr(feature = "merge", merge(strategy = conflate::vec::overwrite_empty))]
    // pub glob_files: Vec<String>,
    // 
    // /// Same as --glob-file ignores the casing of filenames in patterns
    // #[cfg_attr(feature = "clap", clap(long = "iglob-file", value_name = "FILE"))]
    // #[cfg_attr(feature = "merge", merge(strategy = conflate::vec::overwrite_empty))]
    // pub ignore_glob_files: Vec<String>,

    /// Treat the provided filename like a .gitignore file (can be specified multiple times)
    #[cfg_attr(
        feature = "clap",
        clap(long = "custom-ignorefile", value_name = "FILE")
    )]
    #[cfg_attr(feature = "merge", merge(strategy = conflate::vec::overwrite_empty))]
    pub custom_ignore_files: Vec<String>,

    /// Maximum size of files to be backed up. Larger files will be excluded.
    #[cfg_attr(feature = "clap", clap(long, value_name = "SIZE"))]
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[cfg_attr(feature = "merge", merge(strategy = conflate::option::overwrite_none))]
    pub exclude_larger_than: Option<ByteSize>,
}
