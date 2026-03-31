use dashmap::DashMap;
use ignore::overrides::Override;
use ignore::{Match, Walk, WalkBuilder, overrides::OverrideBuilder};
use log::warn;
use rustic_core::{DataFile, DataFilterOptions, DataLocation};
use std::io;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::str::FromStr;

pub struct DataFilter {
    options: DataFilterOptions,
    filter: Override,
    special_dirs: DashMap<PathBuf, bool>, // true = whitelist, false = ignored
}

impl DataFilter {
    pub fn to_walker(self, paths: &[impl AsRef<Path>], recursive: bool) -> io::Result<WalkBuilder> {
        let path = paths.get(0).ok_or(io::Error::new(
            ErrorKind::Other,
            "No paths are present in the filter.",
        ))?;
        let mut walk_builder = WalkBuilder::new(path);
        for path in &paths[1..] {
            _ = walk_builder.add(path);
        }
        _ = walk_builder
            .follow_links(false)
            .hidden(false)
            .ignore(false)
            .sort_by_file_path(Path::cmp)
            .same_file_system(true)
            .max_filesize(self.options.exclude_larger_than.map(|s| s.as_u64()))
            .overrides(self.filter)
            .max_depth(if recursive { None } else { Some(1) });
        Ok(walk_builder)
    }

    /// Determines if a file/directory should be yielded.
    pub fn filter_ok(&self, file: &DataFile) -> io::Result<bool> {
        let meta = file.metadata();
        let path = &meta.path;

        // 1️⃣ Check override patterns
        match self.filter.matched(path, meta.is_dir) {
            Match::Ignore(_) => {
                if meta.is_dir {
                    self.special_dirs.insert(path.to_path_buf(), false);
                }
                return Ok(false);
            }
            Match::Whitelist(_) => {
                if meta.is_dir {
                    self.special_dirs.insert(path.to_path_buf(), true);
                }
                return Ok(true);
            }
            Match::None => {}
        }

        // 2️⃣ Check parent directories for ignore/whitelist
        let mut ancestor = path.parent();
        while let Some(parent) = ancestor {
            if let Some(flag) = self.special_dirs.get(parent).map(|x| *x.value()) {
                return Ok(flag);
            }
            ancestor = parent.parent();
        }

        // 3️⃣ Check custom filename ignores
        if let Some(name) = file.name().to_str() {
            if self.options.custom_ignore_files.contains(&name.to_string()) {
                return Ok(false);
            }
        }

        // 4️⃣ Check file size
        if let Some(max_size) = self.options.exclude_larger_than {
            if meta.size > max_size.as_u64() {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Builds the filter from globs, iglobs, and external glob files
    pub fn new(options: &DataFilterOptions) -> io::Result<Self> {
        let mut override_builder = OverrideBuilder::new("");

        // Helper to read lines from a file and add them to the builder
        // Add normal globs
        for g in &options.globs {
            override_builder
                .add(g)
                .map_err(|err| io::Error::new(ErrorKind::Other, err))?;
        }

        // Case-insensitive for i-globs
        override_builder
            .case_insensitive(true)
            .map_err(|err| io::Error::new(ErrorKind::Other, err))?;

        // Add i-globs
        for g in &options.ignore_globs {
            override_builder
                .add(g)
                .map_err(|err| io::Error::new(ErrorKind::Other, err))?;
        }

        let filter = override_builder
            .build()
            .map_err(|err| io::Error::new(ErrorKind::Other, err))?;

        Ok(Self {
            filter,
            options: options.clone(),
            special_dirs: DashMap::new(),
        })
    }
}
