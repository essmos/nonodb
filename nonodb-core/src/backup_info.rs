//! Human-readable backup information written next to every backup payload.

use std::path::Path;

use crate::error::Result;
use crate::verify::BackupManifest;

/// File name written next to `manifest.json`.
pub const INFO_FILE: &str = "backup-info.txt";

/// Renders a plain-text summary suitable for operators and restore tooling.
pub fn render(manifest: &BackupManifest) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let _ = writeln!(out, "NonoDB backup information");
    let _ = writeln!(out, "========================");
    let _ = writeln!(out);
    let _ = writeln!(out, "Created at (unix ms): {}", manifest.created_at_ms);
    let _ = writeln!(out, "Database: {}", manifest.source);
    if manifest.job_id.is_empty() {
        let _ = writeln!(out, "Backup job: (not scheduled)");
    } else {
        let _ = writeln!(out, "Backup job: {}", manifest.job_id);
    }
    let _ = writeln!(out, "Manifest format: {}", manifest.version);
    let _ = writeln!(out, "Data file: {}", manifest.data_file);
    let _ = writeln!(out, "Data size (bytes): {}", manifest.data_size);
    let _ = writeln!(out, "SHA-256: {}", manifest.sha256);
    let _ = writeln!(
        out,
        "Compression: {}",
        manifest
            .compression
            .as_ref()
            .map(|item| item.kind.as_str())
            .unwrap_or("none")
    );
    let _ = writeln!(
        out,
        "Encryption: {}",
        manifest
            .encryption
            .as_ref()
            .map(|item| item.kind.as_str())
            .unwrap_or("none")
    );
    if manifest.excluded_collections.is_empty() {
        let _ = writeln!(out, "Scope: full database");
        let _ = writeln!(out, "Excluded collections: none");
    } else {
        let _ = writeln!(out, "Scope: partial database copy");
        let _ = writeln!(
            out,
            "Excluded collections: {}",
            manifest.excluded_collections.join(", ")
        );
    }
    if let Some(size) = manifest.plain_size {
        let _ = writeln!(out, "Plaintext size (bytes): {size}");
    }
    if let Some(hash) = &manifest.plain_sha256 {
        let _ = writeln!(out, "Plaintext SHA-256: {hash}");
    }
    let _ = writeln!(out);
    let _ = writeln!(out, "Verify:");
    if manifest.encryption.is_some() {
        let _ = writeln!(
            out,
            "  verify-backup does not support encrypted payloads; restore with --verify or use the Import wizard"
        );
    } else {
        let _ = writeln!(out, "  nonodb-cli verify-backup \"<this directory>\"");
    }
    let _ = writeln!(out);
    let _ = writeln!(out, "Restore:");
    if manifest.encryption.is_some() {
        let _ = writeln!(
            out,
            "  NONODB_BACKUP_IDENTITY_FILE=<identity> nonodb-cli restore \"<this directory>\" \"<data-directory>\" --force --verify"
        );
    } else {
        let _ = writeln!(
            out,
            "  nonodb-cli restore \"<this directory>\" \"<data-directory>\" --force --verify"
        );
    }
    let _ = writeln!(
        out,
        "  or use the NonoDB GUI Import wizard to import as a new database."
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "Warning:");
    let _ = writeln!(
        out,
        "  Backups and encryption are provided as-is, without warranty. Verify every backup"
    );
    let _ = writeln!(
        out,
        "  with a real restore test. Protect encryption keys separately from backups."
    );
    out
}

/// Writes `backup-info.txt` atomically inside a backup directory.
pub fn write(backup_dir: impl AsRef<Path>, manifest: &BackupManifest) -> Result<()> {
    let dir = backup_dir.as_ref();
    let tmp = dir.join(".backup-info.txt.tmp");
    std::fs::write(&tmp, render(manifest))?;
    std::fs::rename(&tmp, dir.join(INFO_FILE))?;
    Ok(())
}
