//! Streaming compression helpers for backup payloads.

use std::fs::File;
use std::io;
use std::path::Path;

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;

use crate::error::Result;

/// Compresses `input` to gzip `output`.
pub fn gzip_file(input: &Path, output: &Path) -> Result<()> {
    let mut reader = File::open(input)?;
    let mut writer = GzEncoder::new(File::create(output)?, Compression::default());
    io::copy(&mut reader, &mut writer)?;
    writer.finish()?;
    Ok(())
}

/// Decompresses gzip `input` to `output`.
pub fn gunzip_file(input: &Path, output: &Path) -> Result<()> {
    let mut reader = GzDecoder::new(File::open(input)?);
    let mut writer = File::create(output)?;
    io::copy(&mut reader, &mut writer)?;
    writer.sync_all()?;
    Ok(())
}
