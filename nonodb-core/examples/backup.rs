//! Database backup (LMDB snapshot) - safe even with a running
//! daemon because it opens the environment read-only.
//!
//! Usage:
//!     cargo run --release -p nonodb-core --example backup -- ./data ./backup
//!
//! `out-dir` is the target directory; the backup is written as `out-dir/data.mdb`,
//! so it can be opened normally (directory + `data.mdb` + `lock.mdb`).
//! `CompactionOption::Enabled` also skips free pages (compacts the file).

use std::error::Error;
use std::path::PathBuf;

use nonodb_core::heed::{CompactionOption, EnvFlags, EnvOpenOptions};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let data = PathBuf::from(args.next().ok_or("użycie: backup <data-dir> <out-dir>")?);
    let out_dir = PathBuf::from(args.next().ok_or("użycie: backup <data-dir> <out-dir>")?);
    std::fs::create_dir_all(&out_dir)?;
    let out = out_dir.join("data.mdb");

    // SAFETY: opened read-only; the file is not modified outside LMDB.
    let env = unsafe {
        EnvOpenOptions::new()
            .flags(EnvFlags::READ_ONLY)
            .open(&data)?
    };

    let file = env.copy_to_file(&out, CompactionOption::Enabled)?;
    file.sync_all()?;
    eprintln!("backup: {} -> {}", data.display(), out.display());
    Ok(())
}
