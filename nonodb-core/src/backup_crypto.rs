//! Streaming encryption helpers for backup payloads.
//!
//! The on-disk format is standard age (X25519 recipients), so backups can
//! also be decrypted with the `age` CLI if the daemon is unavailable.

use std::fs::File;
use std::io;
use std::path::Path;

use age::secrecy::ExposeSecret;

use crate::error::{Error, Result};

/// Encrypts `input` to `output` using an age X25519 recipient
/// (`age1...`). The recipient is public key material.
pub fn encrypt_file(input: &Path, output: &Path, recipient: &str) -> Result<()> {
    let recipient: age::x25519::Recipient = recipient
        .trim()
        .parse()
        .map_err(|err| Error::Query(format!("invalid age recipient: {err}")))?;
    let encryptor =
        age::Encryptor::with_recipients(std::iter::once(&recipient as &dyn age::Recipient))
            .map_err(|err| Error::Query(format!("age recipient error: {err}")))?;
    let mut reader = File::open(input)?;
    let mut writer = encryptor
        .wrap_output(File::create(output)?)
        .map_err(|err| Error::Query(format!("age encryption error: {err}")))?;
    io::copy(&mut reader, &mut writer)?;
    writer
        .finish()
        .map_err(|err| Error::Query(format!("age encryption finish error: {err}")))?;
    Ok(())
}

/// Decrypts an age file using a secret identity (`AGE-SECRET-KEY-...`).
pub fn decrypt_file(input: &Path, output: &Path, identity: &str) -> Result<()> {
    let identity: age::x25519::Identity = identity
        .trim()
        .parse()
        .map_err(|err| Error::Query(format!("invalid age identity: {err}")))?;
    let decryptor = age::Decryptor::new(File::open(input)?)
        .map_err(|err| Error::Query(format!("age header error: {err}")))?;
    let mut reader = decryptor
        .decrypt(std::iter::once(&identity as &dyn age::Identity))
        .map_err(|err| Error::Query(format!("age decryption error: {err}")))?;
    let mut writer = File::create(output)?;
    io::copy(&mut reader, &mut writer)?;
    Ok(())
}

/// Generates an age X25519 keypair as `(identity, recipient)` strings.
pub fn generate_keypair() -> (String, String) {
    let identity = age::x25519::Identity::generate();
    let recipient = identity.to_public().to_string();
    let secret = identity.to_string();
    (secret.expose_secret().to_string(), recipient)
}

/// Reads a single-line age recipient or identity file.
pub fn read_key_file(path: &Path) -> Result<String> {
    let raw = std::fs::read_to_string(path)?;
    let value = raw
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with('#'))
        .ok_or_else(|| Error::Query(format!("empty age key file: {}", path.display())))?;
    Ok(value.to_string())
}
