use std::str::FromStr;
use bytesize::ByteSize;
use rustic_core::{ErrorKind, RusticError, RusticResult};

/// Throttling parameters
///
/// Note: Throttle implements [`FromStr`] to read it from something like "10kiB,10MB"
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Throttle {
    pub bandwidth: u32,
    pub burst: u32,
}

impl FromStr for Throttle {
    type Err = Box<RusticError>;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut values = s
            .split(',')
            .map(|s| {
                ByteSize::from_str(s.trim()).map_err(|err| {
                    RusticError::with_source(
                        ErrorKind::InvalidInput,
                        "Parsing ByteSize from throttle string `{string}` failed",
                        err,
                    )
                        .attach_context("string", s)
                })
            })
            .map(|b| -> RusticResult<u32> {
                let byte_size = b?.as_u64();
                byte_size.try_into().map_err(|err| {
                    RusticError::with_source(
                        ErrorKind::Internal,
                        "Converting ByteSize `{bytesize}` to u32 failed",
                        err,
                    )
                        .attach_context("bytesize", byte_size.to_string())
                })
            });

        let bandwidth = values
            .next()
            .transpose()?
            .ok_or_else(|| RusticError::new(ErrorKind::MissingInput, "No bandwidth given."))?;

        let burst = values
            .next()
            .transpose()?
            .ok_or_else(|| RusticError::new(ErrorKind::MissingInput, "No burst given."))?;

        Ok(Self { bandwidth, burst })
    }
}