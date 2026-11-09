//! Kopia zapasowa bazy (snapshot LMDB) — bezpieczna także przy działającym
//! daemonie, bo otwiera środowisko tylko do odczytu.
//!
//! Użycie:
//!     cargo run --release -p nonodb-core --example backup -- ./data ./backup
//!
//! `out-dir` to katalog docelowy; plik kopii zapisujemy jako `out-dir/data.mdb`,
//! żeby dało się go otworzyć normalnie (katalog + `data.mdb` + `lock.mdb`).
//! `CompactionOption::Enabled` dodatkowo pomija wolne strony (kompaktuje plik).

use std::error::Error;
use std::path::PathBuf;

use nonodb_core::heed::{CompactionOption, EnvFlags, EnvOpenOptions};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let data = PathBuf::from(args.next().ok_or("użycie: backup <data-dir> <out-dir>")?);
    let out_dir = PathBuf::from(args.next().ok_or("użycie: backup <data-dir> <out-dir>")?);
    std::fs::create_dir_all(&out_dir)?;
    let out = out_dir.join("data.mdb");

    // SAFETY: otwieramy tylko do odczytu, nie modyfikujemy pliku poza LMDB.
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
