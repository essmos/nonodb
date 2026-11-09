//! Otwieranie LMDB i zarządzanie kolekcjami.

use std::path::Path;

use heed::types::Bytes;
use heed::{Database, Env, EnvOpenOptions, RoTxn, RwTxn};

use crate::collection::Collection;
use crate::error::{Error, Result};

/// Maksymalny rozmiar pliku bazy. To przestrzeń adresowa mmap — **nie**
/// rezerwacja dysku ani RAM. Plik rośnie w miarę potrzeb do tego limitu.
pub const MAP_SIZE: usize = 1024 * 1024 * 1024; // 1 GiB

/// Rozmiar mapy z `NONODB_MAP_SIZE` (np. `4GiB`, `2048MiB`, bajty).
/// `map_size` to tylko rezerwacja przestrzeni adresowej — nie dysk ani RAM.
fn configured_map_size() -> usize {
    std::env::var("NONODB_MAP_SIZE")
        .ok()
        .and_then(|raw| parse_size(&raw))
        .filter(|&n| n >= PAGE_SIZE)
        .unwrap_or(MAP_SIZE)
}

fn parse_size(raw: &str) -> Option<usize> {
    let s = raw.trim().to_ascii_lowercase();
    const UNITS: [(&str, u64); 6] = [
        ("gib", 1 << 30),
        ("gb", 1_000_000_000),
        ("mib", 1 << 20),
        ("mb", 1_000_000),
        ("kib", 1 << 10),
        ("kb", 1_000),
    ];
    for (suffix, mult) in UNITS {
        if let Some(num) = s.strip_suffix(suffix) {
            let value: f64 = num.trim().parse().ok()?;
            let bytes = value * mult as f64;
            return (bytes >= 1.0 && bytes <= usize::MAX as f64).then_some(bytes as usize);
        }
    }
    s.parse().ok()
}
/// Domyślny rozmiar strony LMDB (4 KiB).
pub const PAGE_SIZE: usize = 4096;
/// Maksymalna liczba sub-baz (kolekcje + meta + indeksy).
pub const MAX_DBS: u32 = 256;
/// Nazwa sub-bazy z metadanymi.
pub const META_DB: &str = "meta";
/// Wersja formatu danych (do przyszłych migracji).
pub const SCHEMA_VERSION: u32 = 1;
/// Soft limit rozmiaru dokumentu (serializowanego JSON).
/// LMDB technicznie obsługuje większe wartości (overflow pages), ale duży
/// dokument = drogi update (przepisanie całości).
pub const MAX_DOCUMENT_BYTES: usize = 1024 * 1024;

/// Uchwyt do bazy. Jeden `Store` = jeden `Env` = jeden plik LMDB.
pub struct Store {
    env: Env,
}

impl Store {
    /// Otwiera (lub tworzy) bazę w katalogu `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        std::fs::create_dir_all(path)?;

        // SAFETY: nie modyfikujemy pliku poza LMDB i nie otwieramy go
        // równolegle z innym `map_size` (daemon jest jedynym właścicielem).
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(configured_map_size())
                .max_dbs(MAX_DBS)
                .open(path)?
        };

        let store = Self { env };
        // Po `kill -9` mogą zostać martwe sloty czytelników w tabeli locków.
        let _ = store.env.clear_stale_readers();
        store.init_meta()?;
        Ok(store)
    }

    pub fn env(&self) -> &Env {
        &self.env
    }

    pub fn read_txn(&self) -> Result<RoTxn<'_>> {
        Ok(self.env.read_txn()?)
    }

    pub fn write_txn(&self) -> Result<RwTxn<'_>> {
        Ok(self.env.write_txn()?)
    }

    fn init_meta(&self) -> Result<()> {
        let mut wtxn = self.write_txn()?;
        let meta: Database<Bytes, Bytes> = self.env.create_database(&mut wtxn, Some(META_DB))?;
        if meta.get(&wtxn, b"schema_version")?.is_none() {
            meta.put(
                &mut wtxn,
                b"schema_version",
                SCHEMA_VERSION.to_string().as_bytes(),
            )?;
        }
        wtxn.commit()?;
        Ok(())
    }

    fn validate_name(name: &str) -> Result<()> {
        if name.is_empty() || name.len() > 200 || name.contains('\0') {
            return Err(Error::InvalidCollectionName(name.to_string()));
        }
        Ok(())
    }

    /// Otwiera kolekcję; tworzy ją, jeśli nie istnieje.
    ///
    /// Istniejąca kolekcja otwierana jest tylko transakcją odczytu (bez zapisu).
    pub fn collection(&self, name: &str) -> Result<Collection<'_>> {
        Self::validate_name(name)?;

        // Szybka ścieżka: kolekcja już istnieje — tylko odczyt.
        {
            let rtxn = self.read_txn()?;
            if let Some(db) = self.env.open_database::<Bytes, Bytes>(&rtxn, Some(name))? {
                let meta = self
                    .env
                    .open_database::<Bytes, Bytes>(&rtxn, Some(META_DB))?
                    .ok_or_else(|| Error::Query("meta database missing".into()))?;
                // UWAGA: heed wymaga `commit()` transakcji odczytu, w której
                // otwarto bazę — inaczej kolejne użycie `db` kończy się EINVAL.
                rtxn.commit()?;
                return Ok(self.wrap(name, db, meta));
            }
            rtxn.commit()?;
        }

        // Wolna ścieżka: pierwsze użycie — utwórz kolekcję.
        let mut wtxn = self.write_txn()?;
        let collection = self.collection_in(&mut wtxn, name)?;
        wtxn.commit()?;
        Ok(collection)
    }

    /// Otwiera/tworzy kolekcję w ramach **istniejącej** transakcji zapisu.
    /// Używane przez task piszący (group commit), by nie otwierać zagnieżdżonej
    /// transakcji.
    pub fn collection_in<'e>(&'e self, wtxn: &mut RwTxn<'_>, name: &str) -> Result<Collection<'e>> {
        Self::validate_name(name)?;
        let db = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(name))?;
        let meta = self
            .env
            .create_database::<Bytes, Bytes>(&mut *wtxn, Some(META_DB))?;
        let key = format!("collection:{name}");
        if meta.get(&*wtxn, key.as_bytes())?.is_none() {
            meta.put(&mut *wtxn, key.as_bytes(), b"1")?;
        }
        Ok(self.wrap(name, db, meta))
    }

    /// Otwiera **istniejącą** kolekcję w podanej transakcji odczytu.
    /// Zwraca `None`, jeśli kolekcja nie istnieje (bez tworzenia).
    pub fn collection_at<'e>(
        &'e self,
        rtxn: &RoTxn<'_>,
        name: &str,
    ) -> Result<Option<Collection<'e>>> {
        Self::validate_name(name)?;
        let Some(db) = self.env.open_database::<Bytes, Bytes>(rtxn, Some(name))? else {
            return Ok(None);
        };
        let meta = self
            .env
            .open_database::<Bytes, Bytes>(rtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        Ok(Some(self.wrap(name, db, meta)))
    }

    fn wrap<'e>(
        &'e self,
        name: &str,
        db: Database<Bytes, Bytes>,
        meta: Database<Bytes, Bytes>,
    ) -> Collection<'e> {
        Collection::new(&self.env, db, meta, name.to_string())
    }

    /// Lista zarejestrowanych kolekcji.
    pub fn collections(&self) -> Result<Vec<String>> {
        let rtxn = self.read_txn()?;
        let out = self.collections_in(&rtxn)?;
        rtxn.commit()?;
        Ok(out)
    }

    /// Lista kolekcji w podanej transakcji odczytu.
    pub fn collections_in(&self, rtxn: &RoTxn<'_>) -> Result<Vec<String>> {
        let meta: Database<Bytes, Bytes> = self
            .env
            .open_database(rtxn, Some(META_DB))?
            .ok_or_else(|| Error::Query("meta database missing".into()))?;
        let mut out = Vec::new();
        for item in meta.iter(rtxn)? {
            let (key, _) = item?;
            if let Ok(k) = std::str::from_utf8(key) {
                if let Some(name) = k.strip_prefix("collection:") {
                    out.push(name.to_string());
                }
            }
        }
        out.sort();
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::parse_size;

    #[test]
    fn parses_map_sizes() {
        assert_eq!(parse_size("4GiB"), Some(4 * (1 << 30)));
        assert_eq!(parse_size("2048MiB"), Some(2048 * (1 << 20)));
        assert_eq!(parse_size("512mb"), Some(512_000_000));
        assert_eq!(parse_size("1073741824"), Some(1 << 30));
        assert_eq!(parse_size("nonsense"), None);
    }
}
