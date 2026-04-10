use std::io;
use std::io::ErrorKind;
use std::path::PathBuf;
use arbhx_core::{FilterOptions, Metadata};
use dashmap::DashMap;
use ignore::Match;
use ignore::overrides::{Override, OverrideBuilder};

/// Represents a **stateful** sorting for a [`DataQuery`]
pub struct SimpleIgnore {
    pub(crate) opts: FilterOptions,
    pub(crate) sort: Override,
    pub(crate) work_dirs: DashMap<PathBuf, bool>, // true = whitelist, false = ignored
}

impl SimpleIgnore {
    fn build_sort(opts: &FilterOptions) -> std::io::Result<Override> {
        let mut override_builder = OverrideBuilder::new("");
        for g in &opts.globs {
            override_builder
                .add(g)
                .map_err(|err| std::io::Error::new(ErrorKind::Other, err))?;
        }

        // Case-insensitive for i-globs
        override_builder
            .case_insensitive(true)
            .map_err(|err| std::io::Error::new(ErrorKind::Other, err))?;

        // Add i-globs
        for g in &opts.ignore_globs {
            override_builder
                .add(g)
                .map_err(|err| std::io::Error::new(ErrorKind::Other, err))?;
        }

        let ret = override_builder
            .build()
            .map_err(|err| io::Error::new(ErrorKind::Other, err))?;
        Ok(ret)
    }

    pub fn new(opts: &FilterOptions) -> io::Result<Self> {
        Ok(Self {
            opts: opts.to_owned(),
            sort: Self::build_sort(opts)?,
            work_dirs: DashMap::new(),
        })
    }

    pub(crate) fn filter_ok(&self, meta: &Metadata) -> std::io::Result<bool> {
        let path = meta.path();

        // 1️⃣ Check override patterns
        match self.sort.matched(path, meta.is_dir()) {
            Match::Ignore(_) => {
                if meta.is_dir() {
                    self.work_dirs.insert(path.to_path_buf(), false);
                }
                return Ok(false);
            }
            Match::Whitelist(_) => {
                if meta.is_dir() {
                    self.work_dirs.insert(path.to_path_buf(), true);
                }
                return Ok(true);
            }
            Match::None => {}
        }

        // 2️⃣ Check parent directories for ignore/whitelist
        let mut ancestor = path.parent();
        while let Some(parent) = ancestor {
            if let Some(flag) = self.work_dirs.get(parent).map(|x| *x.value()) {
                return Ok(flag);
            }
            ancestor = parent.parent();
        }

        // 3️⃣ Check custom filename ignores
        if let Some(name) = meta.name().to_str() {
            if self.opts.custom_ignore_files.contains(&name.to_string()) {
                return Ok(false);
            }
        }

        // 4️⃣ Check file size
        if let Some(max_size) = self.opts.exclude_larger_than {
            if meta.size() > max_size {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

